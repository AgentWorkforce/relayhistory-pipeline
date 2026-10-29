/**
 * Public hostname for the ingest (Khaliq, 2026-10-28: sessions-dev.agentrelay.com,
 * DNS on Cloudflare). CODE behind the AWS clearance gate.
 *
 *   - ACM certificate for the hostname, DNS-validated by a CNAME in Cloudflare
 *     (Cloudflare provider, CLOUDFLARE_API_TOKEN from 1Password at deploy time).
 *   - EKS Auto Mode ALB: IngressClassParams + IngressClass + an Ingress in front of
 *     the 96bulls-alleyoop Service (HTTPS only, target type ip, /healthz checks).
 *   - external-dns (Cloudflare) keeps hostname -> ALB current; its token is a
 *     Kubernetes Secret created out of band from 1Password, never in chart values
 *     or Alchemy state.
 */
import * as AWS from "@alchemy.run/aws";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Kubernetes from "@alchemy.run/kubernetes";
import * as Effect from "effect/Effect";
import * as Output from "alchemy/Output";

export interface IngressInputs {
  stage: string;
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
  tags: Record<string, string>;
}

export const ingressStage = (i: IngressInputs) =>
  Effect.gen(function* () {
    const cluster = i.cluster as never;
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
      name: Output.map(cert.domainValidationOptions, (o) => (o?.[0]?.ResourceRecord?.Name ?? "").replace(/\.$/, "")) as unknown as string,
      content: Output.map(cert.domainValidationOptions, (o) => (o?.[0]?.ResourceRecord?.Value ?? "").replace(/\.$/, "")) as unknown as string,
      ttl: 300,
      proxied: false,
    } as never);

    yield* Kubernetes.Manifest("alb-ingressclassparams", {
      cluster,
      manifest: {
        apiVersion: "eks.amazonaws.com/v1",
        kind: "IngressClassParams",
        metadata: { name: "alb-public" },
        spec: { scheme: "internet-facing" },
      },
    });
    yield* Kubernetes.Manifest("alb-ingressclass", {
      cluster,
      manifest: {
        apiVersion: "networking.k8s.io/v1",
        kind: "IngressClass",
        metadata: { name: "alb-public" },
        spec: {
          controller: "eks.amazonaws.com/alb",
          parameters: { apiGroup: "eks.amazonaws.com", kind: "IngressClassParams", name: "alb-public" },
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
            "alb.ingress.kubernetes.io/listen-ports": '[{"HTTPS":443}]',
            "alb.ingress.kubernetes.io/ssl-policy": "ELBSecurityPolicy-TLS13-1-2-2021-06",
            "alb.ingress.kubernetes.io/certificate-arn": cert.certificateArn as unknown as string,
            "alb.ingress.kubernetes.io/healthcheck-path": "/healthz",
            "external-dns.alpha.kubernetes.io/hostname": i.hostname,
          },
        },
        spec: {
          ingressClassName: "alb-public",
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
    return { hostname: i.hostname, certificateArn: cert.certificateArn };
  });
