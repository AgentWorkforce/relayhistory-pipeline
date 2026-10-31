/**
 * In-cluster TLS in front of self-hosted RelayAuth (relayauth #116: the tenant
 * search-token exchange carries an API key and a user token, so it must never
 * cross the pod network in plain HTTP).
 *
 * RelayAuth chart 0.1.0 serves plain HTTP and takes no sidecar, so TLS is
 * terminated by a small digest-pinned proxy pinned to RelayAuth's node:
 *
 *   caller --TLS 1.3--> relayauth-tls:8443 (proxy, same node as RelayAuth) --HTTP--> relayauth:8787
 *
 * The plaintext hop never leaves the node (required pod affinity). A private CA
 * from cert-manager (namespace-scoped Issuers, nothing cluster-wide) signs the
 * Service certificate; it rotates every 90 days and the proxy reads the files
 * on each handshake, so rotation needs no restart. When a RelayAuth chart can
 * serve TLS itself (or take a sidecar), this proxy goes.
 *
 * Who may connect (NetworkPolicy; the chart's own policy is narrowed too):
 *   relayauth:8787      the proxy, and AlleyOop for the public JWKS only
 *   relayauth-tls:8443  pods labelled `relayauth.agentrelay.com/client: tls`
 *                       in the 96bulls and relayhistory namespaces
 */
import { NAMESPACE } from "./hardening.ts";

export const RELAYAUTH_TLS_NAMESPACE = "relayauth";
export const RELAYAUTH_TLS_SERVICE = "relayauth-tls";
export const RELAYAUTH_TLS_PORT = 8443;
export const RELAYAUTH_TLS_URL = `https://${RELAYAUTH_TLS_SERVICE}.${RELAYAUTH_TLS_NAMESPACE}.svc:${RELAYAUTH_TLS_PORT}`;
/** Label a caller pod carries to reach the TLS endpoint. */
export const RELAYAUTH_TLS_CLIENT_LABEL = { "relayauth.agentrelay.com/client": "tls" } as const;
/** Namespaces whose labelled pods may call the TLS endpoint. */
export const RELAYAUTH_TLS_CLIENT_NAMESPACES = [NAMESPACE, "relayhistory"] as const;

export const CERT_MANAGER_CHART = { repo: "https://charts.jetstack.io", chart: "cert-manager", version: "v1.21.3" } as const;
export const CERT_MANAGER_NAMESPACE = "cert-manager";

/** quay.io/jetstack images for v1.21.3, multi-arch index digests resolved on 2026-10-30. */
export const CERT_MANAGER_DIGESTS = {
  controller: "sha256:9f4f96a33c7774cef24466fcd85031997531a03a8d117f151126baa8d1290dcc",
  webhook: "sha256:055ac8e9de6a0ba6763794838fe38944acc650f68ebe1b01dd8be079e580f458",
  cainjector: "sha256:083d0e906f8eb44e255fe4ff817ae54e49f9ed82e22f1011bb4e9e4f4b8a641f",
  acmesolver: "sha256:c8c1225bb2fe8ab37a330b77f0d8c2ee6a41210fcb54fc07ca606ab9a022b8b7",
} as const;

export const TLS_PROXY_IMAGE = {
  repository: "nginxinc/nginx-unprivileged",
  tag: "1.29.3-alpine",
  digest: "sha256:5aea7cc516b419e3526f47dd1531be31a56a046cfe44754d94f9383e13e2ee99",
} as const;

export function certManagerValues() {
  const d = CERT_MANAGER_DIGESTS;
  return {
    crds: { enabled: true, keep: true },
    image: { digest: d.controller },
    webhook: { image: { digest: d.webhook } },
    cainjector: { image: { digest: d.cainjector } },
    acmesolver: { image: { digest: d.acmesolver } },
    // A post-install Helm hook Job; Alchemy renders --no-hooks, so it would be dropped anyway.
    startupapicheck: { enabled: false },
    // Only namespace-scoped Issuers are used; no ACME, no cluster-wide issuer.
    enableCertificateOwnerRef: true,
  };
}

const labels = { "app.kubernetes.io/name": RELAYAUTH_TLS_SERVICE, "app.kubernetes.io/part-of": "relayauth" };
const relayauthPodLabels = { "app.kubernetes.io/name": "relayauth", "app.kubernetes.io/instance": "relayauth" };
const ns = RELAYAUTH_TLS_NAMESPACE;

/** Self-signed root -> private CA -> Service certificate, all inside the relayauth namespace. */
export function relayauthCaManifests() {
  return [
    { apiVersion: "cert-manager.io/v1", kind: "Issuer", metadata: { name: "relayauth-selfsigned", namespace: ns }, spec: { selfSigned: {} } },
    {
      apiVersion: "cert-manager.io/v1",
      kind: "Certificate",
      metadata: { name: "relayauth-ca", namespace: ns },
      spec: {
        isCA: true,
        commonName: "96bulls relayauth in-cluster CA",
        secretName: "relayauth-ca",
        duration: "8760h",
        renewBefore: "720h",
        privateKey: { algorithm: "ECDSA", size: 256, rotationPolicy: "Always" },
        issuerRef: { name: "relayauth-selfsigned", kind: "Issuer" },
      },
    },
    { apiVersion: "cert-manager.io/v1", kind: "Issuer", metadata: { name: "relayauth-ca", namespace: ns }, spec: { ca: { secretName: "relayauth-ca" } } },
    {
      apiVersion: "cert-manager.io/v1",
      kind: "Certificate",
      metadata: { name: RELAYAUTH_TLS_SERVICE, namespace: ns },
      spec: {
        secretName: RELAYAUTH_TLS_SERVICE,
        duration: "2160h",
        renewBefore: "720h",
        dnsNames: [`${RELAYAUTH_TLS_SERVICE}.${ns}.svc`, `${RELAYAUTH_TLS_SERVICE}.${ns}.svc.cluster.local`],
        usages: ["server auth", "digital signature", "key encipherment"],
        privateKey: { algorithm: "ECDSA", size: 256, rotationPolicy: "Always" },
        issuerRef: { name: "relayauth-ca", kind: "Issuer" },
      },
    },
  ];
}

/**
 * TLS 1.3 only. Certificate paths go through a variable, which makes nginx load
 * them per handshake: a rotated Secret takes effect without a restart.
 */
export function tlsProxyConfig(upstream = `http://relayauth.${ns}.svc:8787`) {
  return [
    "pid /tmp/nginx.pid;",
    "worker_processes 1;",
    "events { worker_connections 256; }",
    "http {",
    "  access_log off;",
    "  server_tokens off;",
    "  client_body_temp_path /tmp/client_body;",
    "  proxy_temp_path /tmp/proxy;",
    "  fastcgi_temp_path /tmp/fastcgi;",
    "  uwsgi_temp_path /tmp/uwsgi;",
    "  scgi_temp_path /tmp/scgi;",
    "  map $ssl_server_name $tls_dir { default /etc/relayauth-tls; }",
    "  server {",
    `    listen ${RELAYAUTH_TLS_PORT} ssl;`,
    "    ssl_protocols TLSv1.3;",
    "    ssl_certificate $tls_dir/tls.crt;",
    "    ssl_certificate_key $tls_dir/tls.key;",
    "    client_max_body_size 1m;",
    "    location = /healthz { access_log off; return 200 'ok'; }",
    "    location / {",
    `      proxy_pass ${upstream};`,
    "      proxy_set_header Host relayauth.relayauth.svc:8787;",
    "      proxy_set_header X-Forwarded-Proto https;",
    "    }",
    "  }",
    "}",
    "",
  ].join("\n");
}

export function tlsProxyManifests() {
  const image = `${TLS_PROXY_IMAGE.repository}:${TLS_PROXY_IMAGE.tag}@${TLS_PROXY_IMAGE.digest}`;
  return [
    { apiVersion: "v1", kind: "ConfigMap", metadata: { name: `${RELAYAUTH_TLS_SERVICE}-nginx`, namespace: ns, labels }, data: { "nginx.conf": tlsProxyConfig() } },
    { apiVersion: "v1", kind: "ServiceAccount", metadata: { name: RELAYAUTH_TLS_SERVICE, namespace: ns, labels }, automountServiceAccountToken: false },
    {
      apiVersion: "apps/v1",
      kind: "Deployment",
      metadata: { name: RELAYAUTH_TLS_SERVICE, namespace: ns, labels },
      spec: {
        replicas: 1,
        selector: { matchLabels: labels },
        template: {
          metadata: { labels },
          spec: {
            serviceAccountName: RELAYAUTH_TLS_SERVICE,
            automountServiceAccountToken: false,
            // The plaintext hop to RelayAuth stays on its node.
            affinity: { podAffinity: { requiredDuringSchedulingIgnoredDuringExecution: [{ labelSelector: { matchLabels: relayauthPodLabels }, topologyKey: "kubernetes.io/hostname" }] } },
            securityContext: { runAsNonRoot: true, runAsUser: 101, runAsGroup: 101, seccompProfile: { type: "RuntimeDefault" } },
            containers: [
              {
                name: "tls",
                image,
                imagePullPolicy: "IfNotPresent",
                args: ["nginx", "-g", "daemon off;", "-c", "/etc/nginx-relayauth/nginx.conf"],
                ports: [{ name: "https", containerPort: RELAYAUTH_TLS_PORT, protocol: "TCP" }],
                securityContext: { allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: { drop: ["ALL"] } },
                readinessProbe: { tcpSocket: { port: "https" }, periodSeconds: 10 },
                livenessProbe: { tcpSocket: { port: "https" }, periodSeconds: 20 },
                resources: { requests: { cpu: "10m", memory: "16Mi" }, limits: { memory: "64Mi" } },
                volumeMounts: [
                  { name: "conf", mountPath: "/etc/nginx-relayauth", readOnly: true },
                  { name: "tls", mountPath: "/etc/relayauth-tls", readOnly: true },
                  { name: "tmp", mountPath: "/tmp" },
                ],
              },
            ],
            volumes: [
              { name: "conf", configMap: { name: `${RELAYAUTH_TLS_SERVICE}-nginx` } },
              // Only the leaf key and chain; the CA private key stays in relayauth-ca.
              { name: "tls", secret: { secretName: RELAYAUTH_TLS_SERVICE, items: [{ key: "tls.crt", path: "tls.crt" }, { key: "tls.key", path: "tls.key" }] } },
              { name: "tmp", emptyDir: { sizeLimit: "16Mi" } },
            ],
          },
        },
      },
    },
    {
      apiVersion: "v1",
      kind: "Service",
      metadata: { name: RELAYAUTH_TLS_SERVICE, namespace: ns, labels },
      spec: { type: "ClusterIP", selector: labels, ports: [{ name: "https", port: RELAYAUTH_TLS_PORT, targetPort: "https", protocol: "TCP" }] },
    },
  ];
}

/** relayauth chart `networkPolicy.ingressFrom`: plain HTTP only from the proxy and AlleyOop (JWKS). */
export function relayauthPlainIngressFrom(clientNamespace: string) {
  return [
    { podSelector: { matchLabels: labels } },
    {
      namespaceSelector: { matchLabels: { "kubernetes.io/metadata.name": clientNamespace } },
      podSelector: { matchLabels: { "app.kubernetes.io/instance": "96bulls", "app.kubernetes.io/name": "alleyoop" } },
    },
  ];
}

const np = (name: string, namespace: string, spec: Record<string, unknown>) => ({ apiVersion: "networking.k8s.io/v1", kind: "NetworkPolicy", metadata: { name, namespace }, spec });

/** NetworkPolicies for the TLS path (added to the relayauth default-deny baseline). */
export function relayauthTlsNetworkPolicies() {
  return [
    np("allow-tls-clients", ns, {
      podSelector: { matchLabels: labels },
      policyTypes: ["Ingress"],
      ingress: [
        {
          from: [{ namespaceSelector: { matchExpressions: [{ key: "kubernetes.io/metadata.name", operator: "In", values: [...RELAYAUTH_TLS_CLIENT_NAMESPACES] }] }, podSelector: { matchLabels: { ...RELAYAUTH_TLS_CLIENT_LABEL } } }],
          ports: [{ protocol: "TCP", port: RELAYAUTH_TLS_PORT }],
        },
      ],
    }),
    np("allow-proxy-to-relayauth", ns, {
      podSelector: { matchLabels: labels },
      policyTypes: ["Egress"],
      egress: [{ to: [{ podSelector: { matchLabels: relayauthPodLabels } }], ports: [{ protocol: "TCP", port: 8787 }] }],
    }),
    np("allow-relayauth-from-proxy-and-jwks", ns, {
      podSelector: { matchLabels: relayauthPodLabels },
      policyTypes: ["Ingress"],
      ingress: [{ from: relayauthPlainIngressFrom(NAMESPACE), ports: [{ protocol: "TCP", port: 8787 }] }],
    }),
    // Callers in 96bulls reach the TLS endpoint (egress side of allow-tls-clients).
    np("allow-relayauth-tls-egress", NAMESPACE, {
      podSelector: { matchLabels: { ...RELAYAUTH_TLS_CLIENT_LABEL } },
      policyTypes: ["Egress"],
      egress: [{ to: [{ namespaceSelector: { matchLabels: { "kubernetes.io/metadata.name": ns } }, podSelector: { matchLabels: labels } }], ports: [{ protocol: "TCP", port: RELAYAUTH_TLS_PORT }] }],
    }),
  ];
}
