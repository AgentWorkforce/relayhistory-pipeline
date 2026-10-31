import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { eksChartValues } from "../eks.ts";
import { PINNED_IMAGES, VPC_CIDR } from "../hardening.ts";
import {
  DEFAULT_INGEST_RATE_LIMIT,
  INGEST_LISTEN_PORTS,
  INGEST_SSL_POLICY,
  albLogDeliveryPolicy,
  ingestLoadBalancerAttributes,
  RELAYCAST_0_1_0,
  STORAGE_CLASS,
  assertRelaycastInputs,
  assertSessionDirectoryPreconditions,
  cnpgHoopdbValues,
  cnpgOperatorValues,
  ingestWafRules,
  ingressExposure,
  networkPolicies,
  networkPolicyControllerConfigMap,
  relaycastValues,
  storageClassManifest,
} from "../proof-addons.ts";
import { relayauthValues } from "../relayauth.ts";

const DIGEST = `sha256:${"a".repeat(64)}`;
const base = { stage: "aws-sessions-dev", runtimeSecretName: "96bulls-runtime", lockerBucket: "b", lockerRegion: "us-west-2", tenantOrgId: "o", relayhistoryOrigin: "https://sessions-dev.agentrelay.com", imageRepository: "r", imageDigest: DIGEST };

describe("encrypted storage", () => {
  it("is gp3 with encryption on, retained, and shared by every in-cluster volume", () => {
    const sc = storageClassManifest();
    expect(sc.metadata.name).toBe(STORAGE_CLASS);
    expect(sc.parameters).toEqual({ type: "gp3", encrypted: "true" });
    expect(sc.reclaimPolicy).toBe("Retain");
    expect(relayauthValues({ secretName: "relayauth", clientNamespace: "96bulls" }).persistence.storageClass).toBe(STORAGE_CLASS);
    expect(cnpgHoopdbValues("cnpg").cluster.storage.storageClass).toBe(STORAGE_CLASS);
    expect(relaycastValues({ ...RELAYCAST_0_1_0, baseUrl: "https://relaycast.example.com" }).persistence.storageClass).toBe(STORAGE_CLASS);
  });
});

describe("CloudNativePG proof HoopDb", () => {
  it("runs one Postgres 17 instance, digest-pinned, on the encrypted class, ordered after the operator", () => {
    const v = cnpgHoopdbValues("cnpg");
    expect(v.enabled).toBe(true);
    expect(v.cluster.imageName).toBe(`ghcr.io/cloudnative-pg/postgresql:17-standard-trixie@${PINNED_IMAGES.cnpgPostgres.digest}`);
    expect(v.cluster.annotations).toEqual({ "96bulls.agentrelay.com/cnpg-operator-release": "cnpg" });
    expect(v.backups.enabled).toBe(false);
  });

  it("pins the operator image by digest and installs its CRDs", () => {
    const v = cnpgOperatorValues();
    expect(v.image.tag).toBe(`1.30.1@${PINNED_IMAGES.cnpgOperator.digest}`);
    expect(v.crds.create).toBe(true);
  });

  it("switches the apps to the CNPG Secret only when the backend is cnpg; external keeps Supabase", () => {
    const external = eksChartValues(base, false, "i", "j");
    expect(external.worker.hoopdb).toEqual({ existingSecret: "96bulls-runtime", key: "HOOPDB_URL" });
    expect(external.alleyoop.hoopdb).toEqual({ existingSecret: "96bulls-runtime", key: "HOOPDB_URL" });
    expect(external.worker.migrations.supabaseShim).toBe(false);
    expect(external.hoopdb).toEqual({ enabled: false });

    const cnpg = eksChartValues(base, false, "i", "j", { operatorRelease: "cnpg" });
    expect(cnpg.worker.hoopdb).toEqual({});
    expect(cnpg.alleyoop.hoopdb).toEqual({});
    expect(cnpg.worker.migrations.supabaseShim).toBe(true);
    expect(cnpg.hoopdb).toMatchObject({ enabled: true, cluster: { storage: { storageClass: STORAGE_CLASS } } });
  });
});

describe("Relaycast", () => {
  const inputs = { ...RELAYCAST_0_1_0, baseUrl: "https://relaycast.sessions-dev.agentrelay.com" };

  it("pins 0.1.0 to the released image digest", () => {
    expect(RELAYCAST_0_1_0).toEqual({ chartVersion: "0.1.0", imageDigest: "sha256:5c86c095a335259060dc34ce7aa2e90809267395af915977a1ae608f93a83b98" });
  });

  it("is ClusterIP only with ingress off, digest-pinned, admitting only the 96bulls namespace", () => {
    const v = relaycastValues(inputs);
    expect(v.image.digest).toBe(RELAYCAST_0_1_0.imageDigest);
    expect(v.service.type).toBe("ClusterIP");
    expect(v.ingress.enabled).toBe(false);
    expect(v.networkPolicy.ingressFrom).toEqual([{ namespaceSelector: { matchLabels: { "kubernetes.io/metadata.name": "96bulls" } } }]);
    expect(JSON.stringify(v)).not.toMatch(/password|token|secret|PRIVATE KEY/i);
  });

  it("refuses tag-only images, inexact chart versions and non-https or pathful origins (0.2.0 slots in by input)", () => {
    expect(() => assertRelaycastInputs({ ...inputs, chartVersion: "0.2.0", imageDigest: `sha256:${"b".repeat(64)}` })).not.toThrow();
    expect(() => assertRelaycastInputs({ ...inputs, imageDigest: "8.0.0" })).toThrow(/tag-only/);
    expect(() => assertRelaycastInputs({ ...inputs, chartVersion: "^0.1" })).toThrow(/exact chart version/);
    expect(() => assertRelaycastInputs({ ...inputs, baseUrl: "http://relaycast.example.com" })).toThrow(/https origin/);
    expect(() => assertRelaycastInputs({ ...inputs, baseUrl: "https://relaycast.example.com/x" })).toThrow(/https origin/);
    expect(() => assertRelaycastInputs({ ...inputs, baseUrl: "https://localhost" })).toThrow(/https origin/);
  });
});

describe("NetworkPolicies", () => {
  const all = networkPolicies({ hoopdb: "cnpg", relayauth: true, relaycast: true });
  type Peer = { ipBlock?: { cidr: string; except?: string[] } };
  type Rule = { from?: Peer[]; to?: Peer[]; ports?: { port: number }[] };

  it("default-denies ingress and egress in every namespace it touches", () => {
    for (const namespace of ["96bulls", "relayauth", "relaycast"]) {
      const deny = all.find((p) => p.metadata.namespace === namespace && p.metadata.name === "default-deny");
      expect(deny?.spec).toEqual({ podSelector: {}, policyTypes: ["Ingress", "Egress"] });
    }
  });

  it("never admits ingress from outside the VPC", () => {
    for (const p of all)
      for (const r of ((p.spec as { ingress?: Rule[] }).ingress ?? []))
        for (const f of r.from ?? []) if (f.ipBlock) expect(f.ipBlock.cidr).toBe(VPC_CIDR);
  });

  it("opens internet egress only on named ports and never to instance metadata", () => {
    for (const p of [...all, ...networkPolicies({ hoopdb: "external", relayauth: false, relaycast: false })])
      for (const r of ((p.spec as { egress?: Rule[] }).egress ?? []))
        for (const t of r.to ?? [])
          if (t.ipBlock?.cidr === "0.0.0.0/0") {
            expect(t.ipBlock.except).toEqual(["169.254.169.254/32"]);
            expect(r.ports?.map((x) => x.port).every((port) => [443, 5432, 6543].includes(port))).toBe(true);
          }
  });

  it("reaches the in-cluster HoopDb only from the apps and the CNPG operator", () => {
    const names = all.filter((p) => p.metadata.namespace === "96bulls").map((p) => p.metadata.name);
    expect(names).toContain("allow-hoopdb-from-apps");
    expect(names).toContain("allow-hoopdb-from-operator");
    const external = networkPolicies({ hoopdb: "external", relayauth: false, relaycast: false }).map((p) => p.metadata.name);
    expect(external).not.toContain("allow-hoopdb-from-apps");
  });

  it("turns enforcement on for EKS Auto Mode", () => {
    expect(networkPolicyControllerConfigMap()).toEqual({
      apiVersion: "v1",
      kind: "ConfigMap",
      metadata: { name: "amazon-vpc-cni", namespace: "kube-system" },
      data: { "enable-network-policy-controller": "true" },
    });
  });
});

describe("ingest exposure (public contract)", () => {
  it("is internet-facing by default and keeps the existing class name, so a live ALB is not replaced", () => {
    expect(ingressExposure({})).toEqual({ scheme: "internet-facing", className: "alb-public", rateLimit: DEFAULT_INGEST_RATE_LIMIT });
    expect(ingressExposure({ scheme: "internal" })).toMatchObject({ scheme: "internal", className: "alb-internal" });
    expect(() => ingressExposure({ scheme: "public" as never })).toThrow(/Nothing was created/);
    expect(() => ingressExposure({ rateLimitPer5Min: 5 })).toThrow(/at least 10/);
  });

  it("listens on HTTPS :443 only with the TLS 1.3 policy, and never opens :80", () => {
    expect(JSON.parse(INGEST_LISTEN_PORTS)).toEqual([{ HTTPS: 443 }]);
    expect(INGEST_SSL_POLICY).toBe("ELBSecurityPolicy-TLS13-1-2-2021-06");
    const src = readFileSync(fileURLToPath(new URL("../ingress.ts", import.meta.url)), "utf8");
    expect(src).not.toMatch(/alb\.ingress\.kubernetes\.io\/ssl-redirect"|"HTTP":\s*80/);
    expect(src).toContain('"alb.ingress.kubernetes.io/wafv2-acl-arn": waf.webAclArn');
  });

  it("attaches AWS IP reputation, known bad inputs, the common rule set, a per-IP rate limit and an operator-route block", () => {
    const rules = ingestWafRules(2000) as { Name: string; Priority: number; Action?: unknown; OverrideAction?: unknown; Statement: Record<string, any> }[];
    expect(rules.map((r) => r.Name)).toEqual([
      "AWSManagedRulesAmazonIpReputationList",
      "AWSManagedRulesKnownBadInputsRuleSet",
      "AWSManagedRulesCommonRuleSet",
      "rate-limit-per-ip",
      "block-operator-routes",
    ]);
    expect(new Set(rules.map((r) => r.Priority)).size).toBe(rules.length);
    for (const r of rules.slice(0, 3)) expect(r.OverrideAction).toEqual({ None: {} });
    expect(rules[3]!.Statement.RateBasedStatement).toEqual({ Limit: 2000, AggregateKeyType: "IP", EvaluationWindowSec: 300 });
    expect(rules[3]!.Action).toEqual({ Block: {} });
    expect(rules[4]!.Statement.ByteMatchStatement.SearchString).toBe("/v1/internal/");
    expect(rules[4]!.Action).toEqual({ Block: {} });
  });

  it("counts (does not block) only body-inspecting managed rules; every other managed rule blocks", () => {
    const rules = ingestWafRules(2000) as { Statement: { ManagedRuleGroupStatement?: { RuleActionOverrides?: { Name: string; ActionToUse: unknown }[] } } }[];
    const overrides = rules.flatMap((r) => r.Statement.ManagedRuleGroupStatement?.RuleActionOverrides ?? []);
    expect(overrides.length).toBeGreaterThan(0);
    for (const o of overrides) {
      expect(o.Name).toMatch(/_BODY$/);
      expect(o.ActionToUse).toEqual({ Count: {} });
    }
  });
});

describe("ingest ALB attributes and access logs", () => {
  it("logs to the stage bucket and drops invalid header fields", () => {
    expect(ingestLoadBalancerAttributes("b")).toEqual([
      { key: "access_logs.s3.enabled", value: "true" },
      { key: "access_logs.s3.bucket", value: "b" },
      { key: "access_logs.s3.prefix", value: "ingest" },
      { key: "routing.http.drop_invalid_header_fields.enabled", value: "true" },
    ]);
  });
  it("lets only Elastic Load Balancing write, and only under this account's log prefix", () => {
    const p = albLogDeliveryPolicy("arn:aws:s3:::logs", "500268966557", "us-west-2");
    expect(p.map((s) => s.Principal)).toEqual([{ AWS: "arn:aws:iam::797873946194:root" }, { Service: "logdelivery.elasticloadbalancing.amazonaws.com" }]);
    for (const s of p) expect(s).toMatchObject({ Effect: "Allow", Action: ["s3:PutObject"], Resource: ["arn:aws:s3:::logs/ingest/AWSLogs/500268966557/*"] });
    expect(() => albLogDeliveryPolicy("arn:aws:s3:::logs", "1", "eu-west-1")).toThrow(/Nothing was created/);
  });
});

describe("session directory PostgREST slot (#73)", () => {
  it("requires the in-cluster CNPG HoopDb and RelayAuth TLS first", () => {
    expect(() => assertSessionDirectoryPreconditions({ hoopdb: "external", relayauthTls: true })).toThrow(/cnpg/);
    expect(() => assertSessionDirectoryPreconditions({ hoopdb: "cnpg", relayauthTls: false })).toThrow(/TLS/);
    expect(() => assertSessionDirectoryPreconditions({ hoopdb: "cnpg", relayauthTls: true })).not.toThrow();
  });

  it("allows only search callers -> PostgREST -> CNPG", () => {
    const ps = networkPolicies({ hoopdb: "cnpg", relayauth: true, relaycast: false, sessionDirectory: true }) as any[];
    const toPostgrest = ps.find((p) => p.metadata.name === "allow-postgrest-from-search-clients");
    expect(toPostgrest.spec.podSelector.matchLabels).toEqual({ "app.kubernetes.io/name": "session-directory-postgrest" });
    expect(toPostgrest.spec.ingress).toEqual([{ from: [{ podSelector: { matchLabels: { "96bulls.agentrelay.com/search-client": "true" } } }], ports: [{ protocol: "TCP", port: 3000 }] }]);
    const hoopdb = ps.find((p) => p.metadata.name === "allow-hoopdb-from-apps");
    expect(hoopdb.spec.ingress[0].from).toContainEqual({ podSelector: { matchLabels: { "app.kubernetes.io/name": "session-directory-postgrest" } } });
    const without = networkPolicies({ hoopdb: "cnpg", relayauth: true, relaycast: false }) as any[];
    expect(without.map((p) => p.metadata.name)).not.toContain("allow-postgrest-from-search-clients");
    expect(JSON.stringify(without.find((p) => p.metadata.name === "allow-hoopdb-from-apps"))).not.toContain("postgrest");
  });
});
