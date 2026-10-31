import { describe, expect, it } from "bun:test";
import { networkPolicies } from "../proof-addons.ts";
import {
  CERT_MANAGER_DIGESTS,
  RELAYAUTH_TLS_URL,
  TLS_PROXY_IMAGE,
  certManagerValues,
  relayauthCaManifests,
  tlsProxyConfig,
  tlsProxyManifests,
} from "../relayauth-tls.ts";

type Peer = { ipBlock?: unknown; podSelector?: { matchLabels?: Record<string, string> }; namespaceSelector?: unknown };
type Policy = { metadata: { name: string; namespace: string }; spec: { podSelector: { matchLabels?: Record<string, string> }; ingress?: { from?: Peer[]; ports?: { port: number }[] }[] } };

describe("RelayAuth in-cluster TLS", () => {
  it("serves TLS 1.3 only on the Service DNS name, from a namespace-scoped private CA", () => {
    expect(RELAYAUTH_TLS_URL).toBe("https://relayauth-tls.relayauth.svc:8443");
    const [selfSigned, ca, caIssuer, leaf] = relayauthCaManifests() as any[];
    expect(selfSigned.kind).toBe("Issuer");
    expect(ca.spec.isCA).toBe(true);
    expect(caIssuer.spec).toEqual({ ca: { secretName: "relayauth-ca" } });
    expect(leaf.spec.dnsNames).toEqual(["relayauth-tls.relayauth.svc", "relayauth-tls.relayauth.svc.cluster.local"]);
    expect(leaf.spec.issuerRef).toEqual({ name: "relayauth-ca", kind: "Issuer" });
    expect(relayauthCaManifests().every((m) => m.kind !== "ClusterIssuer")).toBe(true);
    const conf = tlsProxyConfig();
    expect(conf).toContain("ssl_protocols TLSv1.3;");
    expect(conf).not.toMatch(/TLSv1(\.[012])?[; ]/);
    // Certificates load per handshake (variable path): rotation needs no restart.
    expect(conf).toContain("ssl_certificate $tls_dir/tls.crt;");
  });

  it("never mounts the CA private key in the proxy and keeps the plaintext hop on RelayAuth's node", () => {
    const dep = tlsProxyManifests().find((m) => m.kind === "Deployment") as any;
    const spec = dep.spec.template.spec;
    const tls = spec.volumes.find((v: any) => v.name === "tls");
    expect(tls.secret.secretName).toBe("relayauth-tls");
    expect(tls.secret.items.map((i: any) => i.key)).toEqual(["tls.crt", "tls.key"]);
    expect(JSON.stringify(spec)).not.toContain("relayauth-ca");
    expect(spec.affinity.podAffinity.requiredDuringSchedulingIgnoredDuringExecution[0].topologyKey).toBe("kubernetes.io/hostname");
    expect(spec.automountServiceAccountToken).toBe(false);
    expect(spec.containers[0].securityContext).toEqual({ allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: { drop: ["ALL"] } });
    expect(spec.containers[0].image).toBe(`nginxinc/nginx-unprivileged:1.29.3-alpine@${TLS_PROXY_IMAGE.digest}`);
  });

  it("records the cert-manager and proxy digests in versions.lock.json", () => {
    const lock = JSON.parse(require("node:fs").readFileSync(new URL("../../../versions.lock.json", import.meta.url), "utf8")) as { images: Record<string, { digest: string }> };
    for (const [k, d] of Object.entries(CERT_MANAGER_DIGESTS)) expect(lock.images[`cert-manager-${k}`]!.digest).toBe(d);
    expect(lock.images["relayauth-tls-proxy"]!.digest).toBe(TLS_PROXY_IMAGE.digest);
  });

  it("pins every cert-manager image by digest", () => {
    const v = certManagerValues();
    expect([v.image.digest, v.webhook.image.digest, v.cainjector.image.digest, v.acmesolver.image.digest]).toEqual(Object.values(CERT_MANAGER_DIGESTS));
    for (const d of Object.values(CERT_MANAGER_DIGESTS)) expect(d).toMatch(/^sha256:[0-9a-f]{64}$/);
    expect(v.startupapicheck.enabled).toBe(false);
  });

  it("admits plain HTTP to RelayAuth only from the proxy and AlleyOop, and TLS only from labelled callers", () => {
    const ps = networkPolicies({ hoopdb: "external", relayauth: true, relaycast: false }) as unknown as Policy[];
    const inRelayauth = ps.filter((p) => p.metadata.namespace === "relayauth");
    expect(inRelayauth.map((p) => p.metadata.name)).not.toContain("allow-from-96bulls");
    // No relayauth policy admits by VPC CIDR or by whole namespace.
    for (const p of inRelayauth) for (const r of p.spec.ingress ?? []) for (const f of r.from ?? []) {
      expect(f.ipBlock).toBeUndefined();
      expect(f.podSelector).toBeDefined();
    }
    const plain = inRelayauth.find((p) => p.metadata.name === "allow-relayauth-from-proxy-and-jwks")!;
    expect(plain.spec.ingress![0]!.from!.map((f) => f.podSelector!.matchLabels!["app.kubernetes.io/name"])).toEqual(["relayauth-tls", "alleyoop"]);
    const tls = inRelayauth.find((p) => p.metadata.name === "allow-tls-clients")!;
    expect(tls.spec.ingress![0]!.from![0]!.podSelector!.matchLabels).toEqual({ "relayauth.agentrelay.com/client": "tls" });
    expect(tls.spec.ingress![0]!.ports).toEqual([{ protocol: "TCP", port: 8443 }]);
  });
});
