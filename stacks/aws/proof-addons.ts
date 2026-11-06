/**
 * Pure values for the in-cluster pieces of the isolated AWS proof stage:
 * CloudNativePG HoopDb, Relaycast, the shared encrypted StorageClass, the
 * NetworkPolicies and the ingest exposure. No providers are loaded, so the
 * contracts are unit-tested (stacks/aws/__tests__/proof-addons.test.ts) and
 * rendered with `helm template` before anything is applied.
 */
import { storageClassTagParameters } from "./pippen-sandbox.ts";
import { DIGEST_RE, NAMESPACE, PINNED_IMAGES, VPC_AZ_COUNT, VPC_CIDR, assertDigestPinned, networkSubnetCidrs } from "./hardening.ts";
import { relayauthTlsNetworkPolicies } from "./relayauth-tls.ts";

// ---------------------------------------------------------------------------
// Encrypted storage
// ---------------------------------------------------------------------------

/** Encrypted gp3 (EBS, AWS-managed KMS key). Shared by RelayAuth, HoopDb and Relaycast. */
export const STORAGE_CLASS = "96bulls-gp3";

/** Retain everywhere except proof/sandbox teardown, where a destroyed stage leaves no volumes behind. */
/** `volumeTags`: Pippen sandbox only (pippen-sandbox.ts), so pippen-sweeper finds a volume its cluster left behind. */
export function storageClassManifest(reclaimPolicy: "Retain" | "Delete" = "Retain", volumeTags?: Readonly<Record<string, string>>) {
  return {
    apiVersion: "storage.k8s.io/v1",
    kind: "StorageClass",
    metadata: { name: STORAGE_CLASS },
    provisioner: "ebs.csi.eks.amazonaws.com",
    volumeBindingMode: "WaitForFirstConsumer",
    allowVolumeExpansion: true,
    reclaimPolicy,
    parameters: { type: "gp3", encrypted: "true", ...(volumeTags ? storageClassTagParameters(volumeTags) : {}) },
  };
}

// ---------------------------------------------------------------------------
// CloudNativePG (proof HoopDb)
// ---------------------------------------------------------------------------

/** helm-charts deps/pins.txt: cnpg/cloudnative-pg 0.29.1 (operator 1.30.1). */
export const CNPG_OPERATOR_CHART = { repo: "https://cloudnative-pg.github.io/charts", chart: "cloudnative-pg", version: "0.29.1" } as const;
export const CNPG_NAMESPACE = "cnpg-system";
export const HOOPDB_CLUSTER = "hoopdb";

export function cnpgOperatorValues() {
  const img = PINNED_IMAGES.cnpgOperator;
  return {
    crds: { create: true },
    config: { clusterWide: true },
    // The chart renders `<repository>:<tag>`; a `tag@digest` tag pins the bytes.
    image: { repository: img.repository, tag: `${img.tag}@${img.digest}` },
    resources: { requests: { cpu: "50m", memory: "128Mi" } },
  };
}

/**
 * Umbrella `hoopdb` values for the proof: one PG17 instance on the encrypted
 * class. Backups stay off: the pinned `standard` image ships no barman-cloud,
 * so S3 backups need the Barman Cloud plugin (and cert-manager), which is a
 * separate reviewed addition. The proof database is disposable by design.
 */
export function cnpgHoopdbValues(operatorRelease: string) {
  const img = PINNED_IMAGES.cnpgPostgres;
  return {
    enabled: true,
    cluster: {
      instances: 1,
      imageName: `${img.repository}:${img.tag}@${img.digest}`,
      storage: { size: "20Gi", storageClass: STORAGE_CLASS },
      // Orders the Cluster after the operator release (its CRDs and webhooks).
      annotations: { "96bulls.agentrelay.com/cnpg-operator-release": operatorRelease },
    },
    backups: { enabled: false },
  };
}

// ---------------------------------------------------------------------------
// Relaycast
// ---------------------------------------------------------------------------

export const RELAYCAST_NAMESPACE = "relaycast";
/** helm-charts relaycast 0.1.0 (values.yaml image.digest). Takes no secret material. */
export const RELAYCAST_0_1_0 = { chartVersion: "0.1.0", imageDigest: "sha256:5c86c095a335259060dc34ce7aa2e90809267395af915977a1ae608f93a83b98" } as const;
/** helm-charts relaycast 0.2.0 (values.yaml image.digest: relaycast-self-host 12.2.1-selfhost.1). Requires `existingSecret`. */
export const RELAYCAST_0_2_0 = { chartVersion: "0.2.0", imageDigest: "sha256:a05732ad4f3614d057bb1460df40a30e3a9297c8930bfdda6d4e3d6dc9f41762" } as const;
/**
 * The keys of the Relaycast Secret (chart 0.2.0 `secretKeys`, passed explicitly so a
 * changed chart default cannot move them). Created out of band (docs/aws-stage.md).
 */
export const RELAYCAST_SECRET_KEYS = Object.freeze({ internalSecret: "internal-secret", bootstrapSecret: "workspace-bootstrap-secret" });

export interface RelaycastInputs {
  chartVersion: string;
  imageDigest: string;
  /** HTTPS origin Relaycast embeds in signed URLs and A2A cards; the chart requires it even while ClusterIP-only. */
  baseUrl: string;
  /**
   * Chart 0.2.0 and later: the existing Secret in the relaycast namespace holding
   * RELAYCAST_SECRET_KEYS (the chart's required `existingSecret`). Refused for 0.1.x,
   * which reads no secret.
   */
  secretName?: string;
}

/** Whether this chart version takes the `existingSecret` (0.2.0 and later). */
export function relaycastNeedsSecret(chartVersion: string): boolean {
  const [major, minor] = chartVersion.split(".").map(Number);
  return major! > 0 || minor! >= 2;
}

export function assertRelaycastInputs(i: RelaycastInputs | undefined): asserts i is RelaycastInputs {
  if (!i) throw new Error("96bulls: eks.relaycast is missing. Nothing was created.");
  if (!/^\d+\.\d+\.\d+$/.test(i.chartVersion ?? "")) throw new Error(`96bulls: eks.relaycast.chartVersion must be an exact chart version, got ${JSON.stringify(i.chartVersion)}. Nothing was created.`);
  if (!DIGEST_RE.test(i.imageDigest ?? "")) assertDigestPinned("eks.relaycast.imageDigest", i.imageDigest);
  let u: URL;
  try {
    u = new URL(i.baseUrl);
  } catch {
    throw new Error(`96bulls: eks.relaycast.baseUrl ${JSON.stringify(i.baseUrl)} is not a URL. Nothing was created.`);
  }
  if (u.protocol !== "https:" || u.pathname !== "/" || u.search || u.hash || u.username || !u.hostname.includes("."))
    throw new Error(`96bulls: eks.relaycast.baseUrl must be a bare https origin with public DNS, got ${JSON.stringify(i.baseUrl)}. Nothing was created.`);
  if (relaycastNeedsSecret(i.chartVersion)) {
    if (!/^[a-z0-9]([-a-z0-9]*[a-z0-9])?$/.test(i.secretName ?? "") || i.secretName!.length > 253)
      throw new Error(`96bulls: eks.relaycast.secretName is required for chart ${i.chartVersion} (its existingSecret) and must be a Kubernetes Secret name, got ${JSON.stringify(i.secretName)}. Nothing was created.`);
  } else if (i.secretName !== undefined) {
    throw new Error(`96bulls: eks.relaycast.secretName is for chart 0.2.0 and later; chart ${i.chartVersion} reads no secret. Nothing was created.`);
  }
}

/**
 * ClusterIP only, no ingress, one replica on the encrypted class. Chart 0.1.x
 * takes no secret material (values unchanged); 0.2.0 and later read theirs from
 * the existing Secret `secretName` by name and key, never from a value here.
 */
export function relaycastValues(i: RelaycastInputs) {
  return {
    image: { digest: i.imageDigest },
    baseUrl: new URL(i.baseUrl).origin,
    ...(relaycastNeedsSecret(i.chartVersion) ? { existingSecret: i.secretName, secretKeys: { ...RELAYCAST_SECRET_KEYS } } : {}),
    service: { type: "ClusterIP" },
    ingress: { enabled: false },
    persistence: { storageClass: STORAGE_CLASS, size: "10Gi" },
    networkPolicy: {
      enabled: true,
      ingressFrom: [{ namespaceSelector: { matchLabels: { "kubernetes.io/metadata.name": NAMESPACE } } }],
    },
  };
}

// ---------------------------------------------------------------------------
// NetworkPolicies: default deny, explicit allows
// ---------------------------------------------------------------------------

const ns = (name: string) => ({ namespaceSelector: { matchLabels: { "kubernetes.io/metadata.name": name } } });
const tcp = (...ports: number[]) => ports.map((port) => ({ protocol: "TCP", port }));
/** Anywhere except instance metadata: pods never need IMDS (credentials come from Pod Identity). */
const internetExceptImds = { ipBlock: { cidr: "0.0.0.0/0", except: ["169.254.169.254/32"] } };

function np(namespace: string, name: string, spec: Record<string, unknown>) {
  return { apiVersion: "networking.k8s.io/v1", kind: "NetworkPolicy", metadata: { name, namespace }, spec };
}

/**
 * Kubelet probes come from the pod's own node, which the VPC CNI network policy
 * agent admits without a rule. Pod IPs are VPC addresses too, so a VPC-CIDR
 * allow would open a port to every pod in the cluster: it is used only for the
 * ALB's target port and for ports that serve nothing but health.
 */
const baseline = (namespace: string, healthOnlyPorts: number[]) => [
  np(namespace, "default-deny", { podSelector: {}, policyTypes: ["Ingress", "Egress"] }),
  // EKS Auto Mode serves cluster DNS from the node, not from pods, so DNS is allowed by port.
  np(namespace, "allow-dns", { podSelector: {}, policyTypes: ["Egress"], egress: [{ ports: [{ protocol: "UDP", port: 53 }, { protocol: "TCP", port: 53 }] }] }),
  ...(healthOnlyPorts.length
    ? [np(namespace, "allow-health-ports-from-vpc", { podSelector: {}, policyTypes: ["Ingress"], ingress: [{ from: [{ ipBlock: { cidr: VPC_CIDR } }], ports: tcp(...healthOnlyPorts) }] })]
    : []),
];

export interface NetworkPolicyInputs {
  hoopdb: "external" | "cnpg";
  relayauth: boolean;
  relaycast: boolean;
  /** Fence for the session-directory PostgREST (96BullSessions #73); the Deployment itself comes from that PR. */
  sessionDirectory?: boolean;
  /** Where the ALB lives: public subnets (internet-facing, default) or private (internal). */
  ingestScheme?: IngestScheme;
}

// ---------------------------------------------------------------------------
// Session directory PostgREST slot (96BullSessions #48 -> #72 -> #73)
// ---------------------------------------------------------------------------

/** Labels the #73 PostgREST profile must carry, and the label its search callers carry. */
/**
 * The #73 profile's contract with this stack: Service `session-directory-postgrest`
 * in the 96bulls namespace on port 3000, pods labelled as below. The proof
 * harness addresses it as http://session-directory-postgrest.96bulls.svc:3000.
 */
export const SESSION_DIRECTORY_POSTGREST = {
  service: "session-directory-postgrest",
  namespace: NAMESPACE,
  port: 3000,
  labels: { "app.kubernetes.io/name": "session-directory-postgrest" },
  url: `http://session-directory-postgrest.${NAMESPACE}.svc:3000`,
} as const;
export const SEARCH_CLIENT_LABEL = { "96bulls.agentrelay.com/search-client": "true" } as const;

/**
 * Hard preconditions before the search path may run: PostgREST reads the
 * in-cluster CNPG HoopDb, and the tenant token exchange (relayauth #116) only
 * ever goes to RelayAuth over in-cluster TLS (relayauth-tls.ts).
 */
export function assertSessionDirectoryPreconditions(i: { hoopdb: "external" | "cnpg"; relayauthTls: boolean }) {
  if (i.hoopdb !== "cnpg")
    throw new Error('96bulls: eks.sessionDirectory needs eks.hoopdb.backend "cnpg": the proof PostgREST points at the in-cluster HoopDb. Nothing was created.');
  if (!i.relayauthTls)
    throw new Error("96bulls: eks.sessionDirectory needs self-hosted RelayAuth with in-cluster TLS (eks.relayauth): the search-token exchange carries an API key and a user token. Nothing was created.");
}

export function alleyoopIngressSubnets(scheme: IngestScheme) {
  const s = networkSubnetCidrs(VPC_CIDR, VPC_AZ_COUNT);
  return scheme === "internal" ? s.private : s.public;
}

/** Every NetworkPolicy the proof stage applies, grouped by namespace. */
export function networkPolicies(i: NetworkPolicyInputs) {
  const app = (name: string) => ({ matchLabels: { "app.kubernetes.io/instance": "96bulls", "app.kubernetes.io/name": name } });
  const appPods = { matchExpressions: [{ key: "app.kubernetes.io/name", operator: "In", values: ["worker", "alleyoop"] }] };
  const hoopdbPods = { matchLabels: { "cnpg.io/cluster": "hoopdb" } };
  const policies = [
    // 8081: worker health server; 13133: collector health_check. Neither serves data.
    ...baseline(NAMESPACE, [8081, 13133]),
    // Pod Identity agent (link-local, per node), S3 through the gateway endpoint, the EKS API and JWKS: HTTPS only.
    np(NAMESPACE, "allow-pod-identity-agent", { podSelector: {}, policyTypes: ["Egress"], egress: [{ to: [{ ipBlock: { cidr: "169.254.170.23/32" } }], ports: tcp(80) }] }),
    np(NAMESPACE, "allow-https-egress", { podSelector: {}, policyTypes: ["Egress"], egress: [{ to: [internetExceptImds], ports: tcp(443) }] }),
    // Ingest: only the ALB's subnets may reach AlleyOop, so in-cluster traffic cannot bypass the WAF.
    // Internet-facing ALBs sit in the public subnets, where no pod runs. An internal ALB shares the
    // private subnets with pods, which this rule cannot tell apart (documented trade-off).
    np(NAMESPACE, "allow-alleyoop-from-alb", {
      podSelector: app("alleyoop"),
      policyTypes: ["Ingress"],
      ingress: [{ from: alleyoopIngressSubnets(i.ingestScheme ?? "internet-facing").map((cidr) => ({ ipBlock: { cidr } })), ports: tcp(8080) }],
    }),
    // OTLP from the namespace's own workloads only.
    np(NAMESPACE, "allow-otlp-in-namespace", { podSelector: app("otel"), policyTypes: ["Ingress"], ingress: [{ from: [{ podSelector: {} }], ports: tcp(4317, 4318) }] }),
    np(NAMESPACE, "allow-otlp-egress", { podSelector: appPods, policyTypes: ["Egress"], egress: [{ to: [{ podSelector: app("otel") }], ports: tcp(4317, 4318) }] }),
  ];
  const postgrestPods = { matchLabels: { ...SESSION_DIRECTORY_POSTGREST.labels } };
  if (i.hoopdb === "cnpg") {
    const hoopdbClients = [{ podSelector: appPods }, { podSelector: hoopdbPods }, ...(i.sessionDirectory ? [{ podSelector: postgrestPods }] : [])];
    policies.push(
      np(NAMESPACE, "allow-hoopdb-from-apps", { podSelector: hoopdbPods, policyTypes: ["Ingress"], ingress: [{ from: hoopdbClients, ports: tcp(5432) }] }),
      // The CNPG operator manages instances over their status port and Postgres.
      np(NAMESPACE, "allow-hoopdb-from-operator", { podSelector: hoopdbPods, policyTypes: ["Ingress"], ingress: [{ from: [ns(CNPG_NAMESPACE)], ports: tcp(8000, 5432) }] }),
      np(NAMESPACE, "allow-postgres-egress", { podSelector: {}, policyTypes: ["Egress"], egress: [{ to: [{ podSelector: hoopdbPods }], ports: tcp(5432) }] }),
    );
  } else {
    // External HoopDb (Supabase): Postgres and the pooler, apps only.
    policies.push(np(NAMESPACE, "allow-postgres-egress", { podSelector: appPods, policyTypes: ["Egress"], egress: [{ to: [internetExceptImds], ports: tcp(5432, 6543) }] }));
  }
  if (i.sessionDirectory) {
    // search callers -> PostgREST -> CNPG, nothing else.
    policies.push(
      np(NAMESPACE, "allow-postgrest-from-search-clients", { podSelector: postgrestPods, policyTypes: ["Ingress"], ingress: [{ from: [{ podSelector: { matchLabels: { ...SEARCH_CLIENT_LABEL } } }], ports: tcp(SESSION_DIRECTORY_POSTGREST.port) }] }),
      np(NAMESPACE, "allow-search-clients-to-postgrest", { podSelector: { matchLabels: { ...SEARCH_CLIENT_LABEL } }, policyTypes: ["Egress"], egress: [{ to: [{ podSelector: postgrestPods }], ports: tcp(SESSION_DIRECTORY_POSTGREST.port) }] }),
    );
  }
  if (i.relayauth) {
    policies.push(np(NAMESPACE, "allow-relayauth-egress", { podSelector: app("alleyoop"), policyTypes: ["Egress"], egress: [{ to: [ns("relayauth")], ports: tcp(8787) }] }));
    // Plain HTTP only from the TLS proxy and AlleyOop (public JWKS); secrets go through relayauth-tls:8443.
    policies.push(...baseline("relayauth", []), ...relayauthTlsNetworkPolicies());
  }
  if (i.relaycast) {
    policies.push(...baseline(RELAYCAST_NAMESPACE, []), np(RELAYCAST_NAMESPACE, "allow-from-96bulls", { podSelector: {}, policyTypes: ["Ingress"], ingress: [{ from: [ns(NAMESPACE)], ports: tcp(8787) }] }));
  }
  return policies;
}

/**
 * EKS Auto Mode enforces NetworkPolicy only once the VPC CNI network policy
 * controller is switched on with this ConfigMap; without it the policies above
 * are accepted and ignored.
 */
export function networkPolicyControllerConfigMap() {
  return {
    apiVersion: "v1",
    kind: "ConfigMap",
    metadata: { name: "amazon-vpc-cni", namespace: "kube-system" },
    data: { "enable-network-policy-controller": "true" },
  };
}

// ---------------------------------------------------------------------------
// Ingest exposure: the reviewed public contract
// ---------------------------------------------------------------------------

/**
 * The ingest serves customers (and their bots) from the internet, so the ALB is
 * internet-facing by default; `scheme: "internal"` keeps it inside the VPC. The
 * contract either way (docs/aws-stage.md "Ingest exposure"):
 *
 *   - HTTPS :443 only (TLS 1.3/1.2 policy, ACM certificate); no :80 listener, no redirect;
 *   - every route except health checks and the JWKS requires a RelayAuth JWT
 *     (src/alleyoop/routes.ts PUBLIC_INGEST_PATHS, tested);
 *   - a WAFv2 web ACL: AWS IP reputation, known bad inputs, the common rule set,
 *     a per-IP rate limit, and the operator-only routes blocked at the edge.
 */
export type IngestScheme = "internet-facing" | "internal";

export interface IngestExposureInput {
  /** Default "internet-facing" (customers reach the ingest); "internal" = VPC only. */
  scheme?: IngestScheme;
  /** Requests per source IP per 5 minutes before WAF blocks (WAF minimum 10). */
  rateLimitPer5Min?: number;
  /** Explicitly expose only the dedicated-token POST Amp endpoint; default disabled. */
  ampTransportEnabled?: boolean;
}

export const DEFAULT_INGEST_RATE_LIMIT = 2000;

/**
 * external-dns holds the Cloudflare DNS token, so it runs a pinned image, not
 * the chart's tag default. Chart 1.23.0 renders `<repository>:<tag>`; a
 * `tag@digest` tag pins the bytes. Multi-arch index digest of v0.23.0 (the
 * chart's appVersion), resolved from registry.k8s.io on 2026-10-31.
 */
export const EXTERNAL_DNS_CHART = { repo: "https://kubernetes-sigs.github.io/external-dns", chart: "external-dns", version: "1.23.0" } as const;
export const EXTERNAL_DNS_IMAGE = {
  repository: "registry.k8s.io/external-dns/external-dns",
  tag: "v0.23.0",
  digest: "sha256:1854499e2b08cee47c40a08e356bff20dcb192dbcfde8378fc3f900f12cad5a3",
} as const;

export function externalDnsImageValues() {
  return { repository: EXTERNAL_DNS_IMAGE.repository, tag: `${EXTERNAL_DNS_IMAGE.tag}@${EXTERNAL_DNS_IMAGE.digest}`, pullPolicy: "IfNotPresent" };
}
export const INGEST_LISTEN_PORTS = '[{"HTTPS":443}]';
export const INGEST_SSL_POLICY = "ELBSecurityPolicy-TLS13-1-2-2021-06";

export function ingressExposure(i: IngestExposureInput) {
  const scheme = i.scheme ?? "internet-facing";
  if (scheme !== "internet-facing" && scheme !== "internal")
    throw new Error(`96bulls: eks.ingress.scheme must be "internet-facing" or "internal", got ${JSON.stringify(scheme)}. Nothing was created.`);
  const rateLimit = i.rateLimitPer5Min ?? DEFAULT_INGEST_RATE_LIMIT;
  if (!Number.isInteger(rateLimit) || rateLimit < 10 || rateLimit > 2_000_000_000)
    throw new Error(`96bulls: eks.ingress.rateLimitPer5Min must be an integer of at least 10, got ${JSON.stringify(i.rateLimitPer5Min)}. Nothing was created.`);
  if (i.ampTransportEnabled !== undefined && typeof i.ampTransportEnabled !== "boolean")
    throw new Error("96bulls: eks.ingress.ampTransportEnabled must be a boolean. Nothing was created.");
  // The internet-facing class keeps its existing name, so applying this to a live stage does not replace its ALB.
  return { scheme, className: scheme === "internal" ? "alb-internal" : "alb-public", rateLimit, ...(i.ampTransportEnabled ? { ampTransportEnabled: true as const } : {}) };
}

const visibility = (name: string) => ({ SampledRequestsEnabled: true, CloudWatchMetricsEnabled: true, MetricName: name });
const managed = (name: string, priority: number, countRules: string[] = []) => ({
  Name: name,
  Priority: priority,
  Statement: {
    ManagedRuleGroupStatement: {
      VendorName: "AWS",
      Name: name,
      ...(countRules.length ? { RuleActionOverrides: countRules.map((Name) => ({ Name, ActionToUse: { Count: {} } })) } : {}),
    },
  },
  OverrideAction: { None: {} },
  VisibilityConfig: visibility(name),
});

/**
 * Body-inspecting managed rules count instead of block: request bodies are
 * authenticated session transcripts and chunks, which legitimately hold code,
 * paths, URLs and more than 8 KB. URI, query, header and reputation rules block.
 */
export const INGEST_WAF_BODY_RULES_COUNTED = {
  AWSManagedRulesCommonRuleSet: ["SizeRestrictions_BODY", "CrossSiteScripting_BODY", "GenericLFI_BODY", "GenericRFI_BODY", "EC2MetaDataSSRF_BODY"],
  AWSManagedRulesKnownBadInputsRuleSet: ["Log4JRCE_BODY", "JavaDeserializationRCE_BODY"],
} as const;

/**
 * The web ACL's name, description and metric name. WAFv2 restricts all three
 * (no parentheses in a description): see the charset test in
 * stacks/aws/__tests__/aws-names.test.ts.
 */
export function ingestWafIdentity(stage: string) {
  return {
    webAclName: `96bulls-${stage}-ingest`,
    description: `96bulls ${stage}: ingest ALB, managed rules, per-IP rate limit, operator routes blocked`,
    metricName: `96bulls-${stage}-ingest`,
  };
}

export function ingestWafRules(rateLimit: number, ampTransportEnabled = false) {
  const operatorPaths = {
    ByteMatchStatement: {
      FieldToMatch: { UriPath: {} }, PositionalConstraint: "STARTS_WITH", SearchString: "/v1/internal/",
      TextTransformations: [{ Priority: 0, Type: "URL_DECODE" }, { Priority: 1, Type: "LOWERCASE" }],
    },
  };
  const ampPost = { AndStatement: { Statements: [
    { ByteMatchStatement: { FieldToMatch: { UriPath: {} }, PositionalConstraint: "EXACTLY", SearchString: "/v1/internal/amp/operation", TextTransformations: [{ Priority: 0, Type: "NONE" }] } },
    { ByteMatchStatement: { FieldToMatch: { Method: {} }, PositionalConstraint: "EXACTLY", SearchString: "POST", TextTransformations: [{ Priority: 0, Type: "NONE" }] } },
  ] } };
  return [
    managed("AWSManagedRulesAmazonIpReputationList", 0),
    managed("AWSManagedRulesKnownBadInputsRuleSet", 1, [...INGEST_WAF_BODY_RULES_COUNTED.AWSManagedRulesKnownBadInputsRuleSet]),
    managed("AWSManagedRulesCommonRuleSet", 2, [...INGEST_WAF_BODY_RULES_COUNTED.AWSManagedRulesCommonRuleSet]),
    {
      Name: "rate-limit-per-ip",
      Priority: 3,
      Statement: { RateBasedStatement: { Limit: rateLimit, AggregateKeyType: "IP", EvaluationWindowSec: 300 } },
      Action: { Block: {} },
      VisibilityConfig: visibility("rate-limit-per-ip"),
    },
    {
      // Dead-letter list/replay are operator-only (PIPELINE_OPS_SECRET); reach them through port-forward.
      Name: "block-operator-routes",
      Priority: 4,
      Statement: ampTransportEnabled
        ? { AndStatement: { Statements: [operatorPaths, { NotStatement: { Statement: ampPost } }] } }
        : operatorPaths,
      Action: { Block: {} },
      VisibilityConfig: visibility("block-operator-routes"),
    },
  ];
}

/**
 * ALB attributes: access logs to the stage's own bucket, and headers that are
 * not valid HTTP field names dropped before they reach AlleyOop.
 */
export function ingestLoadBalancerAttributes(accessLogBucket: string) {
  return [
    { key: "access_logs.s3.enabled", value: "true" },
    { key: "access_logs.s3.bucket", value: accessLogBucket },
    { key: "access_logs.s3.prefix", value: "ingest" },
    { key: "routing.http.drop_invalid_header_fields.enabled", value: "true" },
  ];
}

/** Elastic Load Balancing's log-delivery accounts for the regions the stage allows (sandbox-rules.ts). */
export const ELB_LOG_ACCOUNTS: Record<string, string> = { "us-east-1": "127311923021", "us-west-1": "027434742980", "us-west-2": "797873946194" };

/** Bucket policy for ALB access logs: ELB may write under ingest/AWSLogs/<account>/ only. */
export function albLogDeliveryPolicy(bucketArn: string, accountId: string, region: string) {
  const elb = ELB_LOG_ACCOUNTS[region];
  if (!elb) throw new Error(`96bulls: no ELB log-delivery account known for ${region}. Nothing was created.`);
  const prefix = `${bucketArn}/ingest/AWSLogs/${accountId}/*`;
  return [
    { Sid: "ElbAccountWritesLogs", Effect: "Allow" as const, Principal: { AWS: `arn:aws:iam::${elb}:root` }, Action: ["s3:PutObject"], Resource: [prefix] },
    { Sid: "ElbServiceWritesLogs", Effect: "Allow" as const, Principal: { Service: "logdelivery.elasticloadbalancing.amazonaws.com" }, Action: ["s3:PutObject"], Resource: [prefix] },
  ];
}
