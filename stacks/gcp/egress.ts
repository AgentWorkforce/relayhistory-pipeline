/**
 * Workload egress restriction for the 96bulls namespace (GKE Dataplane V2 enforces
 * Kubernetes NetworkPolicy).
 *
 * Default deny in both directions, then only what the services use:
 *
 *   every pod        DNS (kube-dns / NodeLocal DNSCache), the GKE metadata server
 *                    (Workload Identity tokens), Google APIs on private.googleapis.com
 *   worker+AlleyOop  the Cloud SQL proxy :5432, Memorystore Redis :6378 (TLS)
 *   AlleyOop         RelayAuth :8787 (JWKS)
 *   Cloud SQL proxy  Cloud SQL's private IP range :3307; ingress :5432 from the apps
 *
 * Google APIs are reachable only on the private.googleapis.com VIPs: a private
 * Cloud DNS zone for googleapis.com, bound to the stage VPC, resolves every
 * *.googleapis.com name there (Private Google Access is on for the subnet), so
 * no rule needs the public internet. Nothing else leaves the namespace: no
 * arbitrary HTTPS, no external DNS resolvers, no other cluster namespace.
 *
 * Kubelet probes and `kubectl port-forward` arrive from the node itself, which
 * Dataplane V2 does not subject to pod NetworkPolicy.
 */

export const NAMESPACE = "96bulls";

/** private.googleapis.com: 199.36.153.8/30, routable only through Private Google Access. */
export const PRIVATE_GOOGLE_APIS_CIDR = "199.36.153.8/30";
export const PRIVATE_GOOGLE_APIS_IPS = ["199.36.153.8", "199.36.153.9", "199.36.153.10", "199.36.153.11"] as const;

/** GKE metadata server: 169.254.169.254:80, and 169.254.169.252:988 behind it on Dataplane V2. */
const METADATA = [
  { cidr: "169.254.169.254/32", port: 80 },
  { cidr: "169.254.169.252/32", port: 988 },
] as const;
/** NodeLocal DNSCache's link-local listener. */
const NODE_LOCAL_DNS = "169.254.20.10/32";

const CIDR = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})\/(\d{1,2})$/;
function privateCidr(label: string, cidr: string): string {
  const m = CIDR.exec(cidr);
  const [a, b] = [Number(m?.[1]), Number(m?.[2])];
  const prefix = Number(m?.[5]);
  const rfc1918 = a === 10 || (a === 172 && b >= 16 && b <= 31) || (a === 192 && b === 168);
  if (!m || !rfc1918 || prefix < 8 || prefix > 32 || m.slice(1, 5).some((o) => Number(o) > 255))
    throw new Error(`96bulls: ${label} must be a private IPv4 CIDR. Nothing was created.`);
  return cidr;
}

type Port = { protocol: "TCP" | "UDP"; port: number };
const tcp = (...ports: number[]): Port[] => ports.map((port) => ({ protocol: "TCP", port }));

function np(name: string, spec: Record<string, unknown>) {
  return {
    apiVersion: "networking.k8s.io/v1",
    kind: "NetworkPolicy",
    metadata: { name, namespace: NAMESPACE, labels: { "app.kubernetes.io/managed-by": "96bulls-stack" } },
    spec,
  };
}

const app = (name: string) => ({ matchLabels: { "app.kubernetes.io/instance": "96bulls", "app.kubernetes.io/name": name } });
const appPods = {
  matchExpressions: [
    { key: "app.kubernetes.io/instance", operator: "In", values: ["96bulls"] },
    { key: "app.kubernetes.io/name", operator: "In", values: ["worker", "alleyoop"] },
  ],
};
const proxyPods = { matchLabels: { app: "cloud-sql-proxy" } };
const nsSelector = (name: string) => ({ matchLabels: { "kubernetes.io/metadata.name": name } });

export interface Bulls96EgressInputs {
  /** Memorystore's reserved range (the Redis host is inside it). */
  redisRange: string;
  /** The private services access range Cloud SQL's private IP is allocated from. */
  cloudSqlRange: string;
}

export function bulls96NetworkPolicies(i: Bulls96EgressInputs) {
  const redis = privateCidr("redisRange", i.redisRange);
  const sql = privateCidr("cloudSqlRange", i.cloudSqlRange);
  return [
    np("default-deny", { podSelector: {}, policyTypes: ["Ingress", "Egress"] }),
    np("allow-dns", {
      podSelector: {},
      policyTypes: ["Egress"],
      egress: [
        {
          to: [
            {
              namespaceSelector: nsSelector("kube-system"),
              podSelector: { matchExpressions: [{ key: "k8s-app", operator: "In", values: ["kube-dns", "node-local-dns"] }] },
            },
            { ipBlock: { cidr: NODE_LOCAL_DNS } },
          ],
          ports: [
            { protocol: "UDP", port: 53 },
            { protocol: "TCP", port: 53 },
          ],
        },
      ],
    }),
    np("allow-gke-metadata", {
      podSelector: {},
      policyTypes: ["Egress"],
      egress: METADATA.map((m) => ({ to: [{ ipBlock: { cidr: m.cidr } }], ports: tcp(m.port) })),
    }),
    np("allow-private-google-apis", {
      podSelector: {},
      policyTypes: ["Egress"],
      egress: [{ to: [{ ipBlock: { cidr: PRIVATE_GOOGLE_APIS_CIDR } }], ports: tcp(443) }],
    }),
    np("allow-apps-to-cloud-sql-proxy", {
      podSelector: appPods,
      policyTypes: ["Egress"],
      egress: [{ to: [{ podSelector: proxyPods }], ports: tcp(5432) }],
    }),
    np("allow-cloud-sql-proxy-from-apps", {
      podSelector: proxyPods,
      policyTypes: ["Ingress"],
      ingress: [{ from: [{ podSelector: appPods }], ports: tcp(5432) }],
    }),
    np("allow-cloud-sql-proxy-to-cloud-sql", {
      podSelector: proxyPods,
      policyTypes: ["Egress"],
      egress: [{ to: [{ ipBlock: { cidr: sql } }], ports: tcp(3307) }],
    }),
    np("allow-apps-to-redis", {
      podSelector: appPods,
      policyTypes: ["Egress"],
      egress: [{ to: [{ ipBlock: { cidr: redis } }], ports: tcp(6378) }],
    }),
    np("allow-alleyoop-to-relayauth", {
      podSelector: app("alleyoop"),
      policyTypes: ["Egress"],
      egress: [
        {
          to: [{ namespaceSelector: nsSelector("relayauth"), podSelector: { matchLabels: { "app.kubernetes.io/name": "relayauth" } } }],
          ports: tcp(8787),
        },
      ],
    }),
  ];
}

/** Private Cloud DNS records that pin every *.googleapis.com name to the private VIPs. */
export const PRIVATE_GOOGLE_APIS_RECORDS = [
  { id: "private-googleapis-a", name: "private.googleapis.com.", type: "A", rrdatas: [...PRIVATE_GOOGLE_APIS_IPS] },
  { id: "private-googleapis-wildcard", name: "*.googleapis.com.", type: "CNAME", rrdatas: ["private.googleapis.com."] },
] as const;
