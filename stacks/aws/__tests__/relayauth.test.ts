import { describe, expect, it } from "bun:test";
import { RELAYAUTH_IN_CLUSTER_JWKS, RELAYAUTH_IN_CLUSTER_URL, relayauthValues } from "../relayauth.ts";

describe("self-hosted RelayAuth values", () => {
  const v = relayauthValues({ secretName: "relayauth", clientNamespace: "96bulls" });

  it("issuer and baseUrl are the in-cluster Service, and the JWKS URL derives from it", () => {
    expect(RELAYAUTH_IN_CLUSTER_URL).toBe("http://relayauth.relayauth.svc:8787");
    expect(v.issuer).toBe(RELAYAUTH_IN_CLUSTER_URL);
    expect(v.baseUrl).toBe(RELAYAUTH_IN_CLUSTER_URL);
    expect(RELAYAUTH_IN_CLUSTER_JWKS).toBe(`${RELAYAUTH_IN_CLUSTER_URL}/.well-known/jwks.json`);
  });

  it("takes key material only from a pre-created Secret and exposes nothing publicly", () => {
    expect(v.existingSecret).toBe("relayauth");
    expect(JSON.stringify(v)).not.toMatch(/PRIVATE KEY|rak_/);
    expect(v.jwksIngress.enabled).toBe(false);
  });

  it("admits plain HTTP only from the TLS proxy and AlleyOop, and stores SQLite on the encrypted gp3 class", () => {
    expect(v.networkPolicy).toEqual({
      enabled: true,
      ingressFrom: [
        { podSelector: { matchLabels: { "app.kubernetes.io/name": "relayauth-tls", "app.kubernetes.io/part-of": "relayauth" } } },
        {
          namespaceSelector: { matchLabels: { "kubernetes.io/metadata.name": "96bulls" } },
          podSelector: { matchLabels: { "app.kubernetes.io/instance": "96bulls", "app.kubernetes.io/name": "alleyoop" } },
        },
      ],
    });
    expect(v.persistence.storageClass).toBe("96bulls-gp3");
  });
});
