/**
 * Hostname for the ingest (Khaliq, 2026-10-28: sessions-dev.agentrelay.com,
 * DNS on Cloudflare). CODE behind the AWS clearance gate.
 *
 *   - ACM certificate for the hostname, DNS-validated by a CNAME in Cloudflare
 *     (Cloudflare provider, CLOUDFLARE_API_TOKEN from 1Password at deploy time).
 *   - EKS Auto Mode ALB: IngressClassParams + IngressClass + an Ingress in front of
 *     the 96bulls-alleyoop Service (HTTPS only, target type ip, /healthz checks).
 *     Internet-facing by default (customers reach the ingest), or internal with
 *     `scheme: "internal"`. Either way: HTTPS :443 only, JWT on every non-health
 *     route, and a WAFv2 web ACL attached (proof-addons.ts `ingestWafRules`).
 *   - external-dns (Cloudflare) keeps hostname -> ALB current; its token is a
 *     Kubernetes Secret created out of band from 1Password, never in chart values
 *     or Alchemy state.
 */
import * as AWS from "@alchemy.run/aws";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Kubernetes from "@alchemy.run/kubernetes";
import * as Effect from "effect/Effect";
import * as Output from "alchemy/Output";
import { denyInsecureTransport } from "./hardening.ts";
import { INGEST_LISTEN_PORTS, INGEST_SSL_POLICY, albLogDeliveryPolicy, ingestLoadBalancerAttributes, ingestWafRules, ingressExposure, type IngestExposureInput } from "./proof-addons.ts";

export interface IngressInputs {
  stage: string;
  /** For the access-log bucket name and its delivery policy. */
  accountId: string;
  /** Proof/sandbox teardown: destroy empties the access-log bucket. */
  purgeOnDestroy?: boolean;
  /** The ALB's region: ACM certificates attach only to load balancers in their own region. */
  region: string;
  cluster: unknown;
  hostname: string;
  cloudflareZoneId: string;
  zoneDomain: string;
  namespace: string;
  serviceName: string;
  servicePort: number;
  /** Kubernetes Secret (in kube-system) holding the Cloudflare DNS token under key `token`. */
  cloudflareTokenSecret: string;
  /**
   * ACM's DNS validation record for the certificate. Alchemy's ACM resource captures
   * domainValidationOptions at create time, before ACM populates them, so the record is
   * read once from ACM (`aws acm describe-certificate`) and passed in. It is stable for
   * the life of the certificate.
   */
  certValidation?: { name: string; value: string };
  /** ALB scheme (default internet-facing) and the WAF per-IP rate limit. */
  exposure?: IngestExposureInput;
  tags: Record<string, string>;
}

export const ingressStage = (i: IngressInputs) =>
  Effect.gen(function* () {
    const cluster = i.cluster as never;
    // Validate before declaring anything.
    const expose = ingressExposure(i.exposure ?? {});
    const waf = yield* AWS.WAFv2.WebACL("ingest-waf", {
      webAclName: `96bulls-${i.stage}-ingest`,
      scope: "REGIONAL",
      defaultAction: { Allow: {} },
      description: `96bulls ${i.stage}: ingest ALB (managed rules, per-IP rate limit, operator routes blocked)`,
      rules: ingestWafRules(expose.rateLimit) as never,
      visibilityConfig: { SampledRequestsEnabled: true, CloudWatchMetricsEnabled: true, MetricName: `96bulls-${i.stage}-ingest` },
      tags: i.tags,
    });
    // ALB access logs: SSE-S3 (ALB log delivery does not support KMS keys), TLS only, 90 days.
    const logBucketName = `96bulls-${i.stage}-alb-logs-${i.accountId}`;
    const logBucketArn = `arn:aws:s3:::${logBucketName}`;
    const albLogs = yield* AWS.S3.Bucket("alb-logs", {
      bucketName: logBucketName,
      ...(i.purgeOnDestroy ? { forceDestroy: true } : {}),
      versioning: "Enabled",
      encryption: { sseAlgorithm: "AES256" },
      publicAccessBlock: { blockPublicAcls: true, ignorePublicAcls: true, blockPublicPolicy: true, restrictPublicBuckets: true },
      policy: [...denyInsecureTransport(logBucketArn), ...albLogDeliveryPolicy(logBucketArn, i.accountId, i.region)] as never,
      lifecycleRules: [
        { ID: "expire-access-logs", Status: "Enabled", Filter: { Prefix: "" }, Expiration: { Days: 90 }, NoncurrentVersionExpiration: { NoncurrentDays: 7 } },
      ] as never,
      tags: i.tags,
    });
    const cert = yield* AWS.ACM.Certificate("ingest-cert", {
      domainName: i.hostname,
      validationMethod: "DNS",
      // Alchemy defaults ACM to us-east-1 (for CloudFront); an ALB needs the cert in its own region.
      region: i.region,
      tags: i.tags,
    });
    // ACM's validation CNAME, unproxied, in Cloudflare.
    yield* Cloudflare.DNS.Record("ingest-cert-validation", {
      zoneId: i.cloudflareZoneId,
      type: "CNAME",
      name: (i.certValidation
        ? i.certValidation.name.replace(/\.$/, "")
        : Output.map(cert.domainValidationOptions, (o) => (o?.[0]?.ResourceRecord?.Name ?? "").replace(/\.$/, ""))) as unknown as string,
      content: (i.certValidation
        ? i.certValidation.value.replace(/\.$/, "")
        : Output.map(cert.domainValidationOptions, (o) => (o?.[0]?.ResourceRecord?.Value ?? "").replace(/\.$/, ""))) as unknown as string,
      ttl: 300,
      proxied: false,
    } as never);

    yield* Kubernetes.Manifest("alb-ingressclassparams", {
      cluster,
      manifest: {
        apiVersion: "eks.amazonaws.com/v1",
        kind: "IngressClassParams",
        metadata: { name: expose.className },
        spec: { scheme: expose.scheme, loadBalancerAttributes: ingestLoadBalancerAttributes(albLogs.bucketName as unknown as string) },
      },
    });
    yield* Kubernetes.Manifest("alb-ingressclass", {
      cluster,
      manifest: {
        apiVersion: "networking.k8s.io/v1",
        kind: "IngressClass",
        metadata: { name: expose.className },
        spec: {
          controller: "eks.amazonaws.com/alb",
          parameters: { apiGroup: "eks.amazonaws.com", kind: "IngressClassParams", name: expose.className },
        },
      },
    });
    yield* Kubernetes.Manifest("ingest-ingress", {
      cluster,
      manifest: {
        apiVersion: "networking.k8s.io/v1",
        kind: "Ingress",
        metadata: {
          name: "96bulls-ingest",
          namespace: i.namespace,
          annotations: {
            "alb.ingress.kubernetes.io/target-type": "ip",
            // HTTPS only: no :80 listener and deliberately no ssl-redirect (which would open one).
            "alb.ingress.kubernetes.io/listen-ports": INGEST_LISTEN_PORTS,
            "alb.ingress.kubernetes.io/ssl-policy": INGEST_SSL_POLICY,
            "alb.ingress.kubernetes.io/wafv2-acl-arn": waf.webAclArn as unknown as string,
            "alb.ingress.kubernetes.io/certificate-arn": cert.certificateArn as unknown as string,
            "alb.ingress.kubernetes.io/healthcheck-path": "/healthz",
            "external-dns.alpha.kubernetes.io/hostname": i.hostname,
          },
        },
        spec: {
          ingressClassName: expose.className,
          rules: [{ host: i.hostname, http: { paths: [{ path: "/", pathType: "Prefix", backend: { service: { name: i.serviceName, port: { number: i.servicePort } } } }] } }],
        },
      },
    } as never);

    // external-dns: Cloudflare provider, only this zone, only records it owns.
    yield* Kubernetes.HelmChart("external-dns", {
      cluster,
      chart: "external-dns",
      repo: "https://kubernetes-sigs.github.io/external-dns",
      version: "1.23.0",
      releaseName: "external-dns",
      namespace: "kube-system",
      values: {
        provider: { name: "cloudflare" },
        sources: ["ingress"],
        domainFilters: [i.zoneDomain],
        policy: "upsert-only",
        txtOwnerId: `96bulls-${i.stage}`,
        // Cloudflare refuses a TXT record at the same name as a CNAME: keep ownership records on a prefix.
        txtPrefix: "_externaldns.",
        env: [{ name: "CF_API_TOKEN", valueFrom: { secretKeyRef: { name: i.cloudflareTokenSecret, key: "token" } } }],
        extraArgs: ["--cloudflare-dns-records-per-page=5000"],
      },
    });
    return { hostname: i.hostname, certificateArn: cert.certificateArn, scheme: expose.scheme, webAclArn: waf.webAclArn };
  });
