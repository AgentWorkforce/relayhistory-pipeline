import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { GCE_LB_SOURCE_RANGES, PRIVATE_GOOGLE_APIS_CIDR, PRIVATE_GOOGLE_APIS_RECORDS, assertInClusterJwks, bulls96NetworkPolicies, relayauthNetworkPolicies } from "../egress.ts";

type Peer = { ipBlock?: { cidr: string; except?: string[] }; podSelector?: unknown; namespaceSelector?: unknown };
type Rule = { to?: Peer[]; from?: Peer[]; ports?: Array<{ protocol: string; port: number }> };
type Policy = ReturnType<typeof bulls96NetworkPolicies>[number] & {
  spec: { podSelector: Record<string, unknown>; policyTypes: string[]; egress?: Rule[]; ingress?: Rule[] };
};

const policies = bulls96NetworkPolicies({ redisRange: "10.80.0.0/20", cloudSqlRange: "10.80.0.0/20" }) as Policy[];
const byName = (n: string) => policies.find((p) => p.metadata.name === n)!;
const egressBlocks = policies.flatMap((p) => (p.spec.egress ?? []).flatMap((r) => (r.to ?? []).flatMap((t) => (t.ipBlock ? [t.ipBlock.cidr] : []))));

describe("96bulls NetworkPolicies", () => {
  it("starts from default deny in both directions for every pod in the namespace", () => {
    const deny = byName("default-deny");
    expect(deny.metadata.namespace).toBe("96bulls");
    expect(deny.spec).toEqual({ podSelector: {}, policyTypes: ["Ingress", "Egress"] });
    for (const p of policies) expect(p.metadata.namespace).toBe("96bulls");
  });

  it("allows no internet egress: every ipBlock is private, link-local, or private.googleapis.com", () => {
    expect(egressBlocks.sort()).toEqual(
      ["10.80.0.0/20", "10.80.0.0/20", "169.254.169.252/32", "169.254.169.254/32", "169.254.20.10/32", PRIVATE_GOOGLE_APIS_CIDR].sort(),
    );
    for (const cidr of egressBlocks) expect(cidr).not.toBe("0.0.0.0/0");
    // Google APIs only on 443.
    expect(byName("allow-private-google-apis").spec.egress).toEqual([
      { to: [{ ipBlock: { cidr: "199.36.153.8/30" } }], ports: [{ protocol: "TCP", port: 443 }] },
    ]);
  });

  it("scopes each data-plane hop to its port and its callers", () => {
    expect(byName("allow-apps-to-redis").spec.egress![0]!.ports).toEqual([{ protocol: "TCP", port: 6378 }]);
    expect(byName("allow-apps-to-cloud-sql-proxy").spec.egress![0]!.ports).toEqual([{ protocol: "TCP", port: 5432 }]);
    expect(byName("allow-cloud-sql-proxy-to-cloud-sql").spec.egress![0]!.ports).toEqual([{ protocol: "TCP", port: 3307 }]);
    expect(byName("allow-cloud-sql-proxy-to-cloud-sql").spec.podSelector).toEqual({ matchLabels: { app: "cloud-sql-proxy" } });
    expect(byName("allow-cloud-sql-proxy-from-apps").spec.ingress![0]!.ports).toEqual([{ protocol: "TCP", port: 5432 }]);
    // Only AlleyOop reaches RelayAuth, and only on 8787 in the relayauth namespace.
    const ra = byName("allow-alleyoop-to-relayauth");
    expect(ra.spec.podSelector).toEqual({ matchLabels: { "app.kubernetes.io/instance": "96bulls", "app.kubernetes.io/name": "alleyoop" } });
    expect(ra.spec.egress![0]!.ports).toEqual([{ protocol: "TCP", port: 8787 }]);
    expect(JSON.stringify(ra.spec.egress)).toContain('"kubernetes.io/metadata.name":"relayauth"');
  });

  it("limits DNS to the cluster resolvers, never an arbitrary server", () => {
    const dns = byName("allow-dns").spec.egress!;
    expect(dns).toHaveLength(1);
    expect(dns[0]!.to!.every((t) => t.namespaceSelector !== undefined || t.ipBlock?.cidr === "169.254.20.10/32")).toBe(true);
    expect(dns[0]!.ports!.map((p) => `${p.protocol}/${p.port}`).sort()).toEqual(["TCP/53", "UDP/53"]);
  });

  it("gives AlleyOop no in-cluster ingress; only the proxy accepts in-cluster connections", () => {
    const ingress = policies.filter((p) => p.spec.policyTypes.includes("Ingress") && p.metadata.name !== "default-deny");
    expect(ingress.map((p) => p.metadata.name)).toEqual(["allow-cloud-sql-proxy-from-apps"]);
  });

  it("requires the in-cluster RelayAuth JWKS endpoint the policy allows", () => {
    expect(() => assertInClusterJwks("j", "http://relayauth.relayauth.svc:8787/.well-known/jwks.json")).not.toThrow();
    expect(() => assertInClusterJwks("j", "http://relayauth.relayauth.svc.cluster.local:8787/.well-known/jwks.json")).not.toThrow();
    for (const bad of [
      "https://auth.example.com/.well-known/jwks.json",
      "http://relayauth.other.svc:8787/.well-known/jwks.json",
      "http://relayauth.relayauth.svc:8443/.well-known/jwks.json",
      "https://relayauth.relayauth.svc:8787/.well-known/jwks.json",
      "not a url",
    ])
      expect(() => assertInClusterJwks("j", bad)).toThrow("Nothing was created");
    const stack = readFileSync(new URL("../alchemy.gcp.run.ts", import.meta.url), "utf8");
    expect(stack).toContain('assertInClusterJwks("bulls96.relayauthJwksUrl", i.bulls96!.relayauthJwksUrl)');
  });

  it("refuses a public, malformed or out-of-range CIDR", () => {
    for (const bad of ["0.0.0.0/0", "8.8.8.0/24", "10.80.0.0", "10.80.0.0/40", "300.1.1.1/8", "10.80.0.0/20 "])
      expect(() => bulls96NetworkPolicies({ redisRange: bad, cloudSqlRange: "10.80.0.0/20" })).toThrow("private IPv4 CIDR");
  });

  it("pins every googleapis.com name to the private VIPs", () => {
    expect(PRIVATE_GOOGLE_APIS_RECORDS).toEqual([
      { id: "private-googleapis-a", name: "private.googleapis.com.", type: "A", rrdatas: ["199.36.153.8", "199.36.153.9", "199.36.153.10", "199.36.153.11"] },
      { id: "private-googleapis-wildcard", name: "*.googleapis.com.", type: "CNAME", rrdatas: ["private.googleapis.com."] },
    ]);
  });

  it("is wired into the stack: private zone on the stage VPC and every policy applied", () => {
    const stack = readFileSync(new URL("../alchemy.gcp.run.ts", import.meta.url), "utf8");
    expect(stack).toContain('GCP.DNS.ManagedZone("private-googleapis"');
    expect(stack).toContain('visibility: "private"');
    expect(stack).toContain("networks: [network.networkName]");
    expect(stack).toContain('"dns.googleapis.com"');
    expect(stack).toContain("bulls96NetworkPolicies({ redisRange: PSA_CIDR, cloudSqlRange: PSA_CIDR })");
  });
});

describe("relayauth NetworkPolicies", () => {
  const internal = relayauthNetworkPolicies({ publicJwksIngress: false }) as Policy[];
  const named = (ps: Policy[], n: string) => ps.find((p) => p.metadata.name === n)!;

  it("default-denies the namespace and allows DNS as its only egress", () => {
    for (const p of internal) expect(p.metadata.namespace).toBe("relayauth");
    expect(named(internal, "default-deny").spec).toEqual({ podSelector: {}, policyTypes: ["Ingress", "Egress"] });
    const egress = internal.filter((p) => p.spec.policyTypes.includes("Egress") && p.metadata.name !== "default-deny");
    expect(egress.map((p) => p.metadata.name)).toEqual(["allow-dns"]);
    expect(egress[0]!.spec.egress![0]!.ports!.map((p) => p.port)).toEqual([53, 53]);
  });

  it("admits :8787 only from the 96Bulls AlleyOop pods", () => {
    const ingress = internal.filter((p) => p.spec.policyTypes.includes("Ingress") && p.metadata.name !== "default-deny");
    expect(ingress.map((p) => p.metadata.name)).toEqual(["allow-alleyoop-to-relayauth"]);
    expect(ingress[0]!.spec.ingress).toEqual([
      {
        from: [
          {
            namespaceSelector: { matchLabels: { "kubernetes.io/metadata.name": "96bulls" } },
            podSelector: { matchLabels: { "app.kubernetes.io/instance": "96bulls", "app.kubernetes.io/name": "alleyoop" } },
          },
        ],
        ports: [{ protocol: "TCP", port: 8787 }],
      },
    ]);
  });

  it("adds the GCE load-balancer ranges on :8787 only when a public JWKS Ingress exists", () => {
    expect(internal.some((p) => p.metadata.name === "allow-gce-lb-to-relayauth")).toBe(false);
    const pub = relayauthNetworkPolicies({ publicJwksIngress: true }) as Policy[];
    const lb = named(pub, "allow-gce-lb-to-relayauth");
    expect(lb.spec.ingress).toEqual([{ from: GCE_LB_SOURCE_RANGES.map((cidr) => ({ ipBlock: { cidr } })), ports: [{ protocol: "TCP", port: 8787 }] }]);
    expect(pub).toHaveLength(internal.length + 1);
  });

  it("is wired into the stack beside the relayauth namespace", () => {
    const stack = readFileSync(new URL("../alchemy.gcp.run.ts", import.meta.url), "utf8");
    expect(stack).toContain("relayauthNetworkPolicies({ publicJwksIngress: Boolean(relayauthConfig.baseUrl) })");
  });
});
