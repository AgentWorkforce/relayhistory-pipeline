/**
 * Self-hosted RelayAuth inside the environment's own cluster (Khaliq, 2026-10-29:
 * "we need the entire stack to be self hostable"). The environment issues and
 * verifies its own tokens; nothing depends on hosted relayauth.dev.
 *
 * Declares: an encrypted gp3 StorageClass for RelayAuth's SQLite volume (EKS
 * Auto Mode ships no default StorageClass) and the released helm-charts
 * `relayauth` chart (one replica, Recreate, RWO PVC kept on uninstall).
 *
 * The issuer is the in-cluster Service URL. Only AlleyOop verifies today and it
 * reaches RelayAuth inside the cluster, so neither JWKS nor the token API is
 * public. Minting a RelayHistory assertion needs the bootstrap API key (scope
 * relayauth:assertion:create:relayhistory) and network access to the Service
 * (`kubectl port-forward`, or an in-cluster courier).
 *
 * Key material never passes through IaC: the Secret `secretName` (keys
 * internal-secret, signing-key.pem, bootstrap-api-key) is created from 1Password
 * by hand or by the secrets sync, as for 96bulls-runtime. See docs/aws-stage.md.
 */
import * as Kubernetes from "@alchemy.run/kubernetes";
import * as Effect from "effect/Effect";

export const RELAYAUTH_NAMESPACE = "relayauth";
export const RELAYAUTH_RELEASE = "relayauth";
export const RELAYAUTH_STORAGE_CLASS = "96bulls-gp3";
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
    // Only AlleyOop's namespace (enforced where the cluster enforces NetworkPolicy).
    networkPolicy: {
      enabled: true,
      ingressFrom: [{ namespaceSelector: { matchLabels: { "kubernetes.io/metadata.name": i.clientNamespace } } }],
    },
    jwksIngress: { enabled: false },
  };
}

export const relayauthStage = (i: RelayAuthInputs) =>
  Effect.gen(function* () {
    if (!/^\d+\.\d+\.\d+$/.test(i.chartVersion)) throw new Error(`96bulls: eks.relayauth.chartVersion must be an exact chart version, got ${JSON.stringify(i.chartVersion)}. Nothing was created.`);
    if (!/^[a-z0-9]([-a-z0-9]*[a-z0-9])?$/.test(i.secretName)) throw new Error(`96bulls: eks.relayauth.secretName must be a Kubernetes Secret name, got ${JSON.stringify(i.secretName)}. Nothing was created.`);
    const cluster = i.cluster as never;
    yield* Kubernetes.Manifest("storageclass-gp3", {
      cluster,
      manifest: {
        apiVersion: "storage.k8s.io/v1",
        kind: "StorageClass",
        metadata: { name: RELAYAUTH_STORAGE_CLASS },
        provisioner: "ebs.csi.eks.amazonaws.com",
        volumeBindingMode: "WaitForFirstConsumer",
        allowVolumeExpansion: true,
        reclaimPolicy: "Retain",
        parameters: { type: "gp3", encrypted: "true" },
      },
    });
    const chart = yield* Kubernetes.HelmChart("relayauth", {
      cluster,
      chart: "oci://ghcr.io/agentworkforce/charts/relayauth",
      version: i.chartVersion,
      releaseName: RELAYAUTH_RELEASE,
      namespace: RELAYAUTH_NAMESPACE,
      createNamespace: true,
      values: relayauthValues(i),
    });
    return { release: chart.releaseName, issuer: RELAYAUTH_IN_CLUSTER_URL, jwksUrl: RELAYAUTH_IN_CLUSTER_JWKS };
  });
