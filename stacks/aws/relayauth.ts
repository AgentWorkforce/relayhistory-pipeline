/**
 * Self-hosted RelayAuth inside the environment's own cluster (Khaliq, 2026-10-29:
 * "we need the entire stack to be self hostable"). The environment issues and
 * verifies its own tokens; nothing depends on hosted relayauth.dev.
 *
 * Declares the released helm-charts `relayauth` chart (one replica, Recreate,
 * RWO PVC kept on uninstall) on the encrypted gp3 StorageClass that eks.ts
 * declares (EKS Auto Mode ships no default StorageClass).
 *
 * The issuer is the in-cluster Service URL. Only AlleyOop verifies today and it
 * reaches RelayAuth inside the cluster, so neither JWKS nor the token API is
 * public. The chart's Service is plain HTTP, so it admits only AlleyOop (for the
 * public JWKS) and the TLS proxy. Every call that carries a key or a user token
 * (minting, the relayauth #116 search-token exchange) goes to
 * https://relayauth-tls.relayauth.svc:8443, TLS 1.3 with a private CA, from pods
 * labelled `relayauth.agentrelay.com/client: tls` (relayauth-tls.ts).
 *
 * Key material never passes through IaC: the Secret `secretName` (keys
 * internal-secret, signing-key.pem, bootstrap-api-key) is created from 1Password
 * by hand or by the secrets sync, as for 96bulls-runtime. See docs/aws-stage.md.
 */
import * as Kubernetes from "@alchemy.run/kubernetes";
import * as Effect from "effect/Effect";
import { STORAGE_CLASS } from "./proof-addons.ts";
import {
  CERT_MANAGER_CHART,
  CERT_MANAGER_NAMESPACE,
  RELAYAUTH_TLS_URL,
  certManagerValues,
  relayauthCaManifests,
  relayauthPlainIngressFrom,
  tlsProxyManifests,
} from "./relayauth-tls.ts";

export const RELAYAUTH_NAMESPACE = "relayauth";
export const RELAYAUTH_RELEASE = "relayauth";
export const RELAYAUTH_STORAGE_CLASS = STORAGE_CLASS;
/** `iss` on every token this RelayAuth mints, and the base of its JWKS URL. Verifiers pin it byte for byte. */
export const RELAYAUTH_IN_CLUSTER_URL = `http://${RELAYAUTH_RELEASE}.${RELAYAUTH_NAMESPACE}.svc:8787`;
export const RELAYAUTH_IN_CLUSTER_JWKS = `${RELAYAUTH_IN_CLUSTER_URL}/.well-known/jwks.json`;

export interface RelayAuthInputs {
  cluster: unknown;
  /** helm-charts `relayauth` chart version (OCI, ghcr.io/agentworkforce/charts). */
  chartVersion: string;
  /** Pre-created Secret in the relayauth namespace. */
  secretName: string;
  /** Namespace whose pods may call RelayAuth (AlleyOop's). */
  clientNamespace: string;
}

export function relayauthValues(i: Pick<RelayAuthInputs, "secretName" | "clientNamespace">) {
  return {
    existingSecret: i.secretName,
    baseUrl: RELAYAUTH_IN_CLUSTER_URL,
    issuer: RELAYAUTH_IN_CLUSTER_URL,
    persistence: { storageClass: RELAYAUTH_STORAGE_CLASS, size: "1Gi" },
    // Plain HTTP only from the TLS proxy (relayauth-tls.ts) and AlleyOop's pods (public JWKS).
    // Anything carrying a key or a user token uses https://relayauth-tls.relayauth.svc:8443.
    networkPolicy: {
      enabled: true,
      ingressFrom: relayauthPlainIngressFrom(i.clientNamespace),
    },
    jwksIngress: { enabled: false },
  };
}

export const relayauthStage = (i: RelayAuthInputs) =>
  Effect.gen(function* () {
    if (!/^\d+\.\d+\.\d+$/.test(i.chartVersion)) throw new Error(`96bulls: eks.relayauth.chartVersion must be an exact chart version, got ${JSON.stringify(i.chartVersion)}. Nothing was created.`);
    if (!/^[a-z0-9]([-a-z0-9]*[a-z0-9])?$/.test(i.secretName)) throw new Error(`96bulls: eks.relayauth.secretName must be a Kubernetes Secret name, got ${JSON.stringify(i.secretName)}. Nothing was created.`);
    const cluster = i.cluster as never;
    const chart = yield* Kubernetes.HelmChart("relayauth", {
      cluster,
      chart: "oci://ghcr.io/agentworkforce/charts/relayauth",
      version: i.chartVersion,
      releaseName: RELAYAUTH_RELEASE,
      namespace: RELAYAUTH_NAMESPACE,
      createNamespace: true,
      values: relayauthValues(i),
    });

    // In-cluster TLS (relayauth-tls.ts): cert-manager, a namespace-scoped private CA and a
    // TLS 1.3 proxy on RelayAuth's node. Each object is ordered after the release it needs.
    const certManager = yield* Kubernetes.HelmChart("cert-manager", {
      cluster,
      chart: CERT_MANAGER_CHART.chart,
      repo: CERT_MANAGER_CHART.repo,
      version: CERT_MANAGER_CHART.version,
      releaseName: "cert-manager",
      namespace: CERT_MANAGER_NAMESPACE,
      createNamespace: true,
      values: certManagerValues(),
    });
    const after = (m: { metadata: Record<string, unknown> }, release: unknown) => ({
      ...m,
      metadata: { ...m.metadata, annotations: { "96bulls.agentrelay.com/after-release": release as string } },
    });
    for (const m of relayauthCaManifests())
      yield* Kubernetes.Manifest(`relayauth-tls-${m.kind.toLowerCase()}-${m.metadata.name}`, { cluster, manifest: after(m, certManager.releaseName) as never });
    for (const m of tlsProxyManifests())
      yield* Kubernetes.Manifest(`relayauth-tls-${m.kind.toLowerCase()}`, { cluster, manifest: after(m, chart.releaseName) as never });
    return { release: chart.releaseName, issuer: RELAYAUTH_IN_CLUSTER_URL, jwksUrl: RELAYAUTH_IN_CLUSTER_JWKS, tlsUrl: RELAYAUTH_TLS_URL };
  });
