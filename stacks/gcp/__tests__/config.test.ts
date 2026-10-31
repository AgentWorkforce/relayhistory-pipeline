import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "bun:test";
import versionsLock from "../../../versions.lock.json";
import {
  assertGcpInputs,
  assertGcpRuntimeDeployable,
  GCP_APPROVED_TARGET,
  GCP_APPROVED_SERVICES_IMAGE,
  GCP_NODE_ROLES,
  GCP_RUNTIME_SHAPE,
  bulls96ReleaseValues,
  gcpResourceName,
  pubsubServiceAgent,
  relayauthChartValues,
  RELAYCAST_PUBLISHED_CHART,
  relaycastChartValues,
  relayfileChartValues,
  relayflowsChartValues,
  requestedAlchemyCommand,
  captureRelaycastChartPackage,
  type GcpInputs,
} from "../config.ts";

const base: GcpInputs = {
  ...GCP_APPROVED_TARGET,
  controlPlaneAuthorizedCidrs: [{ cidrBlock: "203.0.113.10/32", displayName: "operator" }],
  clearedBy: "Khaliq: code review fixture only, 2026-10-29",
  managedDataPlane: { provision: false },
  kubernetesExecutor: {
    name: "gcp-vpn-runner-01",
    mode: "vpn",
    identity: "gcp-vpn-runner@agentrelay-96bulls-soc2-2610.iam.gserviceaccount.com",
  },
};

describe("GCP self-host contract", () => {
  it("pins the reviewed multi-architecture services image for the GCP proof", () => {
    expect(versionsLock.images.services).toMatchObject({
      ref: "us-central1-docker.pkg.dev/agentrelay-96bulls-soc2-2610/awf-conformance-images/96bulls-services",
      digest: "sha256:8d77de9be0b5e5e47dda75986da882e577aafb9e28e510c844e8134fea372425",
      sourceRevision: "09293c8d649f912aa2f4414e31a3ad7fca6eceee",
      gcpProofEligible: true,
    });
  });

  it("accepts a substrate-only stage without pretending managed services are attached", () => {
    expect(() => assertGcpInputs("gcp-dev", base)).not.toThrow();
    expect(() => assertGcpRuntimeDeployable("gcp-dev", base)).not.toThrow();
    expect(() => assertGcpInputs("gcp-../../outside", base)).toThrow(/safe gcp-\* stage/);
  });

  it("does not throw when bulls96 is disabled", () => {
    const disabled = {
      enabled: false,
      chartVersion: "0.0.1",
      tenantOrgId: "https://user:password@example.com",
      existingSecretName: "not a secret name",
      imagePullSecretName: "not a pull secret name",
      imageDigest: "not-a-digest",
      imageSourceRevision: "not-a-revision",
      relayauthJwksUrl: "http://user:password@attacker.example/jwks?token=secret",
      relayauthIssuer: "http://user:password@attacker.example",
      relayhistoryOrigin: "http://user:password@attacker.example",
    };
    expect(() => assertGcpInputs("gcp-dev", { ...base, bulls96: disabled })).not.toThrow();
    expect(() => assertGcpRuntimeDeployable("gcp-dev", { ...base, bulls96: disabled })).not.toThrow();
    expect(() =>
      assertGcpRuntimeDeployable("gcp-dev", {
        ...base,
        bulls96: { ...disabled, enabled: true },
      }),
    ).toThrow(/acknowledged managed data plane/);
    const stack = readFileSync(new URL("../alchemy.gcp.run.ts", import.meta.url), "utf8");
    const start = stack.indexOf("const bulls96 = i.bulls96?.enabled");
    const gate = stack.slice(start, stack.indexOf("return {", start));
    expect(gate.startsWith("const bulls96 = i.bulls96?.enabled")).toBe(true);
    expect(gate).toContain("96Bulls install requires the managed data plane");
    expect(gate.trimEnd().endsWith(": null;")).toBe(true);
  });

  it("rejects any project, number, org, billing account, or region other than the approved target", () => {
    expect(() => assertGcpInputs("gcp-dev", { ...base, projectId: "agentworkforce-dev1" })).toThrow(
      /does not match the approved project/,
    );
    expect(() => assertGcpInputs("gcp-dev", { ...base, projectNumber: "123456789012" })).toThrow(
      /does not match the approved project/,
    );
    expect(() => assertGcpInputs("gcp-dev", { ...base, organizationId: "123456789012" })).toThrow(
      /does not match the approved project/,
    );
    expect(() => assertGcpInputs("gcp-dev", { ...base, billingAccount: "AAAAAA-BBBBBB-CCCCCC" })).toThrow(
      /does not match the approved project/,
    );
    expect(() => assertGcpInputs("gcp-dev", { ...base, region: "us-east1" })).toThrow(
      /does not match the approved project/,
    );
  });

  it("requires an explicit adapter-proof acknowledgement before managed services can cost money", () => {
    expect(() =>
      assertGcpInputs("gcp-conformance", {
        ...base,
        managedDataPlane: { provision: true },
      }),
    ).toThrow("acknowledgement=adapter-proof-only");

    expect(() =>
      assertGcpInputs("gcp-dev", {
        ...base,
        managedDataPlane: { provision: true, acknowledgement: "adapter-proof-only" },
      }),
    ).not.toThrow();
  });

  it("rejects an internet-wide GKE control-plane allowlist", () => {
    expect(() =>
      assertGcpInputs("gcp-dev", {
        ...base,
        controlPlaneAuthorizedCidrs: [{ cidrBlock: "0.0.0.0/0" }],
      }),
    ).toThrow("unsafe or invalid control-plane CIDR");
    for (const cidrBlock of ["999.1.1.1/32", "203.0.113.10 /32", "203.0.113.10/33", "not-an-ip/32"])
      expect(() => assertGcpInputs("gcp-dev", { ...base, controlPlaneAuthorizedCidrs: [{ cidrBlock }] })).toThrow(
        "unsafe or invalid control-plane CIDR",
      );
  });

  it("validates 96Bulls inputs and allows only the acknowledged managed runtime", () => {
    const bulls96 = {
      enabled: true,
      chartVersion: "0.3.0",
      tenantOrgId: "org_123",
      existingSecretName: "bulls96-runtime",
      imageDigest: `sha256:${"a".repeat(64)}`,
      imageSourceRevision: GCP_APPROVED_SERVICES_IMAGE.sourceRevision,
      relayauthJwksUrl: "http://relayauth.relayauth.svc:8787/.well-known/jwks.json",
      relayauthIssuer: "http://relayauth.relayauth.svc:8787",
      relayhistoryOrigin: "http://bulls96-alleyoop.96bulls.svc:8080",
    };
    expect(() => assertGcpInputs("gcp-dev", { ...base, bulls96 })).toThrow("managedDataPlane.provision");
    expect(() =>
      assertGcpInputs("gcp-dev", {
        ...base,
        managedDataPlane: { provision: true, acknowledgement: "adapter-proof-only" },
        bulls96: { ...bulls96, tenantOrgId: "https://user:password@example.com" },
      }),
    ).toThrow("org id");
    expect(() =>
      assertGcpInputs("gcp-dev", {
        ...base,
        managedDataPlane: { provision: true, acknowledgement: "adapter-proof-only" },
        bulls96: { ...bulls96, imageDigest: "main" },
      }),
    ).toThrow("immutable sha256 digest");
    expect(() =>
      assertGcpInputs("gcp-dev", {
        ...base,
        managedDataPlane: { provision: true, acknowledgement: "adapter-proof-only" },
        bulls96: { ...bulls96, imageSourceRevision: "db2663d" },
      }),
    ).toThrow("full Git commit SHA");
    expect(() =>
      assertGcpInputs("gcp-conformance", {
        ...base,
        managedDataPlane: { provision: true, acknowledgement: "adapter-proof-only" },
        bulls96,
      }),
    ).toThrow("does not match the reviewed release");
    expect(() =>
      assertGcpInputs("gcp-dev", {
        ...base,
        managedDataPlane: { provision: true, acknowledgement: "adapter-proof-only" },
        bulls96: { ...bulls96, relayauthJwksUrl: "http://user:password@relayauth.relayauth.svc/jwks" },
      }),
    ).toThrow("must not contain credentials");
    expect(() =>
      assertGcpInputs("gcp-dev", {
        ...base,
        managedDataPlane: { provision: true, acknowledgement: "adapter-proof-only" },
        bulls96: { ...bulls96, relayauthJwksUrl: "http://relayauth.attacker.example/jwks" },
      }),
    ).toThrow("public HTTPS or the in-cluster RelayAuth service");
    expect(() =>
      assertGcpInputs("gcp-dev", {
        ...base,
        managedDataPlane: { provision: true, acknowledgement: "adapter-proof-only" },
        bulls96: { ...bulls96, relayauthJwksUrl: "https://relay.example.com/jwks?token=secret-boundary" },
      }),
    ).toThrow("public HTTPS or the in-cluster RelayAuth service");
    expect(() =>
      assertGcpInputs("gcp-dev", {
        ...base,
        managedDataPlane: { provision: true, acknowledgement: "adapter-proof-only" },
        bulls96: { ...bulls96, relayauthIssuer: "https://relay.example.com/issuer-path" },
      }),
    ).toThrow("RelayAuth origin");
    for (const relayhistoryOrigin of [
      "http://user:password@bulls96-alleyoop.96bulls.svc:8080",
      "http://attacker.example",
      "https://history.example.com/path",
    ])
      expect(() =>
        assertGcpInputs("gcp-dev", {
          ...base,
          managedDataPlane: { provision: true, acknowledgement: "adapter-proof-only" },
          bulls96: { ...bulls96, relayhistoryOrigin },
        }),
      ).toThrow("in-cluster Kubernetes origin");
    expect(() =>
      assertGcpInputs("gcp-dev", {
        ...base,
        managedDataPlane: { provision: true, acknowledgement: "adapter-proof-only" },
        relayauth: { chartVersion: "0.1.0", existingSecretName: "relayauth-runtime", baseUrl: "https://auth.example.com" },
        bulls96,
      }),
    ).toThrow("must match relayauth.baseUrl");
    expect(() =>
      assertGcpInputs("gcp-conformance", {
        ...base,
        managedDataPlane: { provision: true, acknowledgement: "adapter-proof-only" },
        bulls96: { ...bulls96, imageDigest: GCP_APPROVED_SERVICES_IMAGE.digest },
      }),
    ).not.toThrow();
    expect(() =>
      assertGcpRuntimeDeployable("gcp-conformance", {
        ...base,
        managedDataPlane: { provision: true, acknowledgement: "adapter-proof-only" },
        bulls96: { ...bulls96, imageDigest: GCP_APPROVED_SERVICES_IMAGE.digest },
      }),
    ).not.toThrow();
    const approvedRuntime = {
      ...base,
      managedDataPlane: { provision: true, acknowledgement: "adapter-proof-only" as const },
      bulls96: { ...bulls96, imageDigest: GCP_APPROVED_SERVICES_IMAGE.digest },
    };
    expect(() => assertGcpInputs("gcp-dev", approvedRuntime, "plan")).toThrow("published only for gcp-conformance");
    expect(() => assertGcpRuntimeDeployable("gcp-dev", approvedRuntime, "deploy")).toThrow(
      "published only for gcp-conformance",
    );
    expect(() => assertGcpInputs("gcp-dev", approvedRuntime, "destroy")).not.toThrow();
    expect(() => assertGcpRuntimeDeployable("gcp-dev", approvedRuntime, "destroy")).not.toThrow();
    const recordedButIneligible = {
      ...base,
      managedDataPlane: { provision: true, acknowledgement: "adapter-proof-only" as const },
      bulls96: {
        ...bulls96,
        imageDigest: "sha256:245a75f9d7abdbd2167108db6e4fa852df844b41a22eaebcd64b4dd7b753b2aa",
        imageSourceRevision: "db2663d05b327b945f1db40fd3b8aec13726fcef",
      },
    };
    expect(() => assertGcpInputs("gcp-dev", recordedButIneligible, "destroy")).not.toThrow();
    expect(() => assertGcpRuntimeDeployable("gcp-dev", recordedButIneligible, "destroy")).not.toThrow();
    for (const command of ["plan", "deploy"] as const) {
      expect(() => assertGcpInputs("gcp-conformance", recordedButIneligible, command)).toThrow(
        "does not match the reviewed release",
      );
      expect(() => assertGcpRuntimeDeployable("gcp-conformance", recordedButIneligible, command)).toThrow(
        "does not match the reviewed release",
      );
    }
  });

  it("mirrors Alchemy's first-known-command scan for destroy cleanup", () => {
    expect(requestedAlchemyCommand(["--stage", "gcp-dev", "destroy"])).toBe("destroy");
    expect(requestedAlchemyCommand(["--stage", "gcp-dev", "plan", "destroy"])).toBe("plan");
    expect(requestedAlchemyCommand(["--stage", "gcp-dev", "bogus"])).toBeUndefined();
  });

  it("renders a CA-backed Redis round-trip contract without AUTH or HMAC credentials", () => {
    const values = bulls96ReleaseValues(
        {
          enabled: true,
          chartVersion: "0.3.0",
          tenantOrgId: "org_123",
          existingSecretName: "bulls96-runtime",
          imageDigest: `sha256:${"a".repeat(64)}`,
          imageSourceRevision: GCP_APPROVED_SERVICES_IMAGE.sourceRevision,
          relayauthJwksUrl: "http://relayauth.relayauth.svc:8787/.well-known/jwks.json",
          relayauthIssuer: "http://relayauth.relayauth.svc:8787",
          relayhistoryOrigin: "http://bulls96-alleyoop.96bulls.svc:8080",
        },
        {
          bucket: "locker",
          region: "us-central1",
          imageRepository: "us-central1-docker.pkg.dev/agentworkforce-dev1/awf-dev-images/96bulls-services",
          cloudSqlConnectionName: "agentworkforce-dev1:us-central1:awf-dev-postgres",
          redisHost: "10.52.0.3",
          redisPort: 6379,
          redisCaPem: "-----BEGIN CERTIFICATE-----\nQUJDRA==\n-----END CERTIFICATE-----\n",
          pubsubTopic: "projects/agentworkforce-dev1/topics/events",
          pubsubSubscription: "projects/agentworkforce-dev1/subscriptions/events",
          workloadIdentityEmail: "awf-dev-runtime@agentworkforce-dev1.iam.gserviceaccount.com",
        },
      );
    const rendered = JSON.stringify(values);
    expect(rendered).toContain("REDIS_TLS_CA_PEM");
    expect(rendered).toContain("REDIS_TRANSIT_ENCRYPTION");
    expect(rendered).toContain("SERVER_AUTHENTICATION");
    expect(rendered).toContain("us-central1-docker.pkg.dev/agentworkforce-dev1/awf-dev-images/96bulls-services");
    expect(rendered).not.toContain("ghcr-pull");
    expect(rendered).not.toContain('"REDIS_AUTH"');
    expect(rendered).not.toContain("REDIS_PASSWORD");
    expect(rendered).not.toContain("LOCKER_ACCESS_KEY_ID");
    expect(values).toMatchObject({
      worker: {
        fullnameOverride: "bulls96-worker",
        imagePullSecrets: [],
        image: {
          repository: "us-central1-docker.pkg.dev/agentworkforce-dev1/awf-dev-images/96bulls-services",
          digest: `sha256:${"a".repeat(64)}`,
        },
        locker: { credentials: { mode: "podIdentity" } },
      },
      alleyoop: {
        fullnameOverride: "bulls96-alleyoop",
        imagePullSecrets: [],
        image: {
          repository: "us-central1-docker.pkg.dev/agentworkforce-dev1/awf-dev-images/96bulls-services",
          digest: `sha256:${"a".repeat(64)}`,
        },
        locker: { credentials: { mode: "podIdentity" } },
        auth: {
          jwksUrl: "http://relayauth.relayauth.svc:8787/.well-known/jwks.json",
          issuer: "http://relayauth.relayauth.svc:8787",
        },
        extraEnv: expect.arrayContaining([
          {
            name: "RELAYHISTORY_ORIGIN",
            value: "http://bulls96-alleyoop.96bulls.svc:8080",
          },
        ]),
      },
      hoopdb: { enabled: false },
      locker: { enabled: false },
    });
  });

  it("keeps Relaycast on the released single-writer SQLite profile", () => {
    const values = relaycastChartValues({
      chartVersion: "0.1.0",
      baseUrl: "https://relay.gcp.example.com",
    });
    expect(values.replicaCount).toBe(1);
    expect(values.strategy).toEqual({ type: "Recreate" });
    expect(values.image).toEqual({
      repository: RELAYCAST_PUBLISHED_CHART.repository,
      digest: RELAYCAST_PUBLISHED_CHART.imageDigest,
    });
    expect(values.runtime.backend).toBe("sqlite");
    expect(values.persistence.accessMode).toBe("ReadWriteOnce");
    expect(values.ingress).toEqual({ enabled: false, className: "", annotations: {}, hosts: [], tls: [] });
    expect(values.service).toEqual({ type: "ClusterIP", annotations: {} });
    expect(values.networkPolicy).toEqual({ enabled: true, ingressFrom: [] });
  });

  it("keeps shortened resource names unique across long stage suffixes", () => {
    const one = gcpResourceName("gcp-conformance-integration-dev-one", "runtime", 30);
    const two = gcpResourceName("gcp-conformance-integration-dev-two", "runtime", 30);
    expect(one).not.toBe(two);
    expect(one).toHaveLength(30);
    expect(two).toHaveLength(30);
    expect(one.endsWith("-runtime")).toBe(true);
    expect(two.endsWith("-runtime")).toBe(true);
    expect(one).toMatch(/^[a-z][a-z0-9-]{4,28}[a-z0-9]$/);
    expect(two).toMatch(/^[a-z][a-z0-9-]{4,28}[a-z0-9]$/);
  });

  it("uses the Pub/Sub service agent identity for dead-letter IAM", () => {
    expect(pubsubServiceAgent("123456789012")).toBe(
      "service-123456789012@gcp-sa-pubsub.iam.gserviceaccount.com",
    );
    expect(() => pubsubServiceAgent("project-name")).toThrow("projectNumber is invalid");
  });

  it("posts a provider-compatible private runtime shape", () => {
    expect(GCP_RUNTIME_SHAPE.privateEndpoint).toBe(true);
    expect(GCP_RUNTIME_SHAPE.autopilot).toBe(false);
    expect(GCP_RUNTIME_SHAPE.nodeLocation).toBe("us-central1-a");
    expect(GCP_RUNTIME_SHAPE.natIpAllocateOption).toBe("MANUAL_ONLY");
    expect(GCP_RUNTIME_SHAPE.natLogFilter).toBe("ALL");
    expect(GCP_NODE_ROLES).toEqual([
      "roles/container.defaultNodeServiceAccount",
      "roles/artifactregistry.reader",
    ]);
    const stack = readFileSync(new URL("../alchemy.gcp.run.ts", import.meta.url), "utf8");
    expect(stack).toContain("enablePrivateEndpoint: GCP_RUNTIME_SHAPE.privateEndpoint");
    expect(stack).toContain("autopilot: GCP_RUNTIME_SHAPE.autopilot");
    expect(stack).toContain("serviceAccount: nodeActAs.serviceAccountEmail");
    expect(stack).toContain('GCP.IAM.Member("node-artifactregistry-reader"');
    expect(stack).toContain("Output.all(images.registryUri, nodeArtifactRegistryReader.role)");
    expect(stack.indexOf('GCP.IAM.Member("node-artifactregistry-reader"')).toBeLessThan(
      stack.indexOf('Kubernetes.HelmChart("96bulls"'),
    );
    expect(stack.indexOf('RequireNodeActAs("node-act-as"')).toBeLessThan(
      stack.indexOf('GCP.Container.Cluster("cluster"'),
    );
    expect(stack).toContain("nodeLocations: [GCP_RUNTIME_SHAPE.nodeLocation]");
    expect(stack).toContain("natIpAllocateOption: GCP_RUNTIME_SHAPE.natIpAllocateOption");
    expect(stack).toContain("filter: GCP_RUNTIME_SHAPE.natLogFilter");
    expect(stack).toContain("purpose: \"VPC_PEERING\"");
    expect(stack).toContain("network: network.selfLink.as<string>()");
    expect(stack).toContain("authorizedNetwork: Output.map(");
    expect(stack).toContain('`projects/${i.projectId}/global/networks/${name}`');
    expect(stack).toContain(
      'const project = /^projects\\/([^/]+)\\/global\\/networks\\//.exec(input.spec.privateNetwork)?.[1];',
    );
    expect(stack).toContain('versionDestroyTtl: afterApi("secretmanager.googleapis.com"');
    expect(stack).toContain("Output.map(cluster.workloadPool");
    expect(stack.indexOf('RequireKubernetesReachability("kubernetes-reachability"')).toBeLessThan(
      stack.indexOf('Kubernetes.Manifest("relaycast-namespace"'),
    );
    expect(stack).not.toContain("AUTO_ONLY");
    expect(stack).not.toContain("enablePrivateEndpoint: false");
    expect(stack).not.toContain("Shared VPC");
  });

  it("gates the mutation graph on a fresh, globally single-use executor proof", () => {
    const stack = readFileSync(new URL("../alchemy.gcp.run.ts", import.meta.url), "utf8");
    const applyAuthorization = readFileSync(new URL("../apply-authorization.ts", import.meta.url), "utf8");
    const authorization = stack.indexOf('RequireGcpApplyAuthorization("apply-authorization"');
    const firstGcpMutation = stack.indexOf("GCP.ServiceUsage.ProjectService(");
    expect(authorization).toBeGreaterThan(0);
    expect(authorization).toBeLessThan(firstGcpMutation);
    expect(stack).toContain("consumeGcpApplyNonceGlobally(");
    expect(applyAuthorization).toContain('url.searchParams.set("ifGenerationMatch", "0")');
    expect(stack).toContain("Output.map(applyAuthorization.proof");
    expect(stack).toContain("authorization: applyAuthorization");
    expect(stack.indexOf('RequireNodeActAs("node-act-as"')).toBeLessThan(
      stack.indexOf('GCP.Container.Cluster("cluster"'),
    );
    expect(stack.indexOf('RequireKubernetesReachability("kubernetes-reachability"')).toBeLessThan(
      stack.indexOf('Kubernetes.Manifest("relaycast-namespace"'),
    );
  });

  it("requires the named private-route executor before any chart is allowed", () => {
    expect(() =>
      assertGcpInputs("gcp-dev", {
        ...base,
        kubernetesExecutor: undefined,
        relaycast: { chartVersion: "0.1.0", baseUrl: "https://relay.gcp.example.com" },
      }),
    ).toThrow(/named executor identity and fresh signed nonce/);
    expect(() =>
      assertGcpInputs("gcp-dev", {
        ...base,
        kubernetesExecutor: { name: "gcp-vpn-runner-01", mode: "vpn", identity: "not-a-service-account" },
      }),
    ).toThrow(/Google service-account email/);
  });

  it("rejects a missing or digest-mismatched Relaycast package", () => {
    const dir = mkdtempSync(join(tmpdir(), "96bulls-chart-test-"));
    try {
      const path = join(dir, "relaycast-0.1.0.tgz");
      expect(() => captureRelaycastChartPackage(path)).toThrow(/package is missing/);
      writeFileSync(path, "not-the-published-chart");
      expect(() => captureRelaycastChartPackage(path)).toThrow(/digest does not match/);
      const stack = readFileSync(new URL("../alchemy.gcp.run.ts", import.meta.url), "utf8");
      const cache = readFileSync(new URL("../chart-cache.ts", import.meta.url), "utf8");
      expect(stack).toContain("captureRelaycastChartPackage");
      expect(stack).toContain('process.env.CI !== "true"');
      expect(cache).toContain("writeFileSync(artifact, captured.bytes");
      expect(cache).not.toContain("readFileSync(captured.sourcePath");
      expect(cache).not.toContain("rmSync(extracted");
      expect(stack).toContain("invocationId: APPLY_INVOCATION_ID");
      expect(stack).toContain('chart: relaycastChart!');
      expect(stack).not.toContain('chart: "relaycast"');
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("rejects an unpublished Relaycast chart version or repository", () => {
    expect(() =>
      assertGcpInputs("gcp-dev", {
        ...base,
        relaycast: { chartVersion: "0.2.0", baseUrl: "https://relay.gcp.example.com" },
      }),
    ).toThrow(/published chart 0\.1\.0/);
    expect(() =>
      assertGcpInputs("gcp-dev", {
        ...base,
        chartRepo: "https://example.com/charts",
        relaycast: { chartVersion: "0.1.0", baseUrl: "https://relay.gcp.example.com" },
      }),
    ).toThrow(/published chart 0\.1\.0/);
    expect(() =>
      relaycastChartValues({ chartVersion: "0.2.0", baseUrl: "https://relay.gcp.example.com" }),
    ).toThrow(/published 0\.1\.0/);
  });

  it("rejects credential-bearing public origins before they reach chart values", () => {
    expect(() =>
      assertGcpInputs("gcp-dev", {
        ...base,
        relaycast: {
          chartVersion: "0.1.0",
          baseUrl: "https://user:password@relay.gcp.example.com",
        },
      }),
    ).toThrow("without credentials");
  });

  it("rejects IP and special-use local Relaycast origins before chart rendering", () => {
    for (const baseUrl of ["https://127.0.0.1", "https://relay.example.local"]) {
      expect(() =>
        assertGcpInputs("gcp-dev", {
          ...base,
          relaycast: { chartVersion: "0.1.0", baseUrl },
        }),
      ).toThrow("public multi-label DNS hostname");
    }
  });

  it("passes only existing Secret names to charts", () => {
    const auth = relayauthChartValues({
      chartVersion: "0.1.0",
      existingSecretName: "relayauth-runtime",
    });
    const file = relayfileChartValues({
      chartVersion: "0.3.0",
      existingSecretName: "relayfile-runtime",
      relayauthJwksUrl: "http://relayauth.relayauth.svc:8787/.well-known/jwks.json",
    });
    const rendered = JSON.stringify({ auth, file });
    expect(rendered).toContain("relayauth-runtime");
    expect(rendered).toContain("relayfile-runtime");
    expect(rendered).not.toMatch(/PRIVATE KEY|password|token/i);
  });

  it("keeps Relayflows standalone and optional", () => {
    expect(
      relayflowsChartValues({
        chartVersion: "0.4.0",
        flowConfigMap: "nightly-flow",
      }),
    ).toMatchObject({
      mode: "standalone",
      standalone: { flow: { existingConfigMap: "nightly-flow", configMapKey: "flow.yaml" } },
    });
  });
});
