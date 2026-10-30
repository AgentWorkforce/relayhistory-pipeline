import { readFileSync } from "node:fs";
import { describe, expect, it } from "bun:test";
import {
  assertGcpInputs,
  assertGcpRuntimeDeployable,
  GCP_APPROVED_TARGET,
  GCP_NODE_ROLES,
  GCP_RUNTIME_SHAPE,
  bulls96ReleaseValues,
  gcpResourceName,
  pubsubServiceAgent,
  relayauthChartValues,
  GCP_REDIS_CLIENT,
  RELAYCAST_PUBLISHED_CHART,
  relaycastChartValues,
  relayfileChartValues,
  relayflowsChartValues,
  type GcpInputs,
} from "../config.ts";

const base: GcpInputs = {
  ...GCP_APPROVED_TARGET,
  controlPlaneAuthorizedCidrs: [{ cidrBlock: "203.0.113.10/32", displayName: "operator" }],
  clearedBy: "Khaliq: code review fixture only, 2026-10-29",
  managedDataPlane: { provision: false },
};

describe("GCP self-host contract", () => {
  it("accepts a substrate-only stage without pretending managed services are attached", () => {
    expect(() => assertGcpInputs("gcp-dev", base)).not.toThrow();
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
      assertGcpInputs("gcp-dev", {
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
  });

  it("installs 96Bulls only with the managed data plane and non-secret chart inputs", () => {
    const bulls96 = {
      enabled: true,
      chartVersion: "0.3.0",
      tenantOrgId: "org_123",
      existingSecretName: "bulls96-runtime",
      relayauthJwksUrl: "http://relayauth.relayauth.svc:8787/.well-known/jwks.json",
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
        bulls96,
      }),
    ).not.toThrow();
    expect(() =>
      assertGcpRuntimeDeployable({
        ...base,
        managedDataPlane: { provision: true, acknowledgement: "adapter-proof-only" },
        bulls96,
      }),
    ).not.toThrow();
  });

  it("wires GCS, SQL, Redis, and Pub/Sub into the Helm release without secrets", () => {
    const values = bulls96ReleaseValues(
      {
        enabled: true,
        chartVersion: "0.3.0",
        tenantOrgId: "org_123",
        existingSecretName: "bulls96-runtime",
        relayauthJwksUrl: "http://relayauth.relayauth.svc:8787/.well-known/jwks.json",
      },
      {
        bucket: "awf-dev-locker-agentworkforce-dev1",
        region: "us-central1",
        cloudSqlConnectionName: "agentworkforce-dev1:us-central1:awf-dev-postgres",
        redisHost: "10.52.0.3",
        redisPort: 6379,
        pubsubTopic: "projects/agentworkforce-dev1/topics/awf-dev-events",
        pubsubSubscription: "projects/agentworkforce-dev1/subscriptions/awf-dev-events-worker",
        workloadIdentityEmail: "awf-dev-runtime@agentworkforce-dev1.iam.gserviceaccount.com",
      },
    );
    const rendered = JSON.stringify(values);
    expect(values).toMatchObject({
      hoopdb: { enabled: false },
      locker: { enabled: false },
      worker: {
        locker: { bucket: "awf-dev-locker-agentworkforce-dev1", credentials: { mode: "podIdentity" } },
        hoopdb: { existingSecret: "bulls96-runtime", key: "HOOPDB_URL" },
      },
      alleyoop: {
        auth: { jwksUrl: "http://relayauth.relayauth.svc:8787/.well-known/jwks.json" },
        locker: { credentials: { mode: "podIdentity" } },
      },
    });
    expect(rendered).toContain("LOCKER_PROVIDER");
    expect(rendered).toContain('"gcs"');
    expect(rendered).toContain("CLOUD_SQL_CONNECTION_NAME");
    expect(rendered).toContain("REDIS_HOST");
    expect(rendered).toContain("REDIS_AUTH_ENABLED");
    expect(rendered).toContain(GCP_REDIS_CLIENT.authEnabled);
    expect(rendered).toContain(GCP_REDIS_CLIENT.transitEncryption);
    expect(rendered).toContain("PUBSUB_TOPIC");
    expect(rendered).toContain("WORKER_DATA_PATH_PROBE");
    expect(rendered).not.toContain("LOCKER_ACCESS_KEY_ID");
    expect(rendered).not.toContain('"REDIS_AUTH"');
    expect(rendered).not.toContain("REDIS_PASSWORD");
    expect(rendered).not.toMatch(/password|BEGIN |hmac/i);
    expect(() =>
      bulls96ReleaseValues(
        {
          enabled: true,
          chartVersion: "0.3.0",
          tenantOrgId: "org_123",
          existingSecretName: "bulls96-runtime",
          relayauthJwksUrl: "http://relayauth.relayauth.svc:8787/.well-known/jwks.json",
        },
        {
          bucket: "locker",
          region: "us-central1",
          cloudSqlConnectionName: "postgres://user:secret@127.0.0.1/hoopdb",
          redisHost: "10.52.0.3",
          redisPort: 6379,
          pubsubTopic: "projects/agentworkforce-dev1/topics/events",
          pubsubSubscription: "projects/agentworkforce-dev1/subscriptions/events",
          workloadIdentityEmail: "awf-dev-runtime@agentworkforce-dev1.iam.gserviceaccount.com",
        },
      ),
    ).toThrow("Nothing was created.");
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
    expect(values.ingress.annotations["networking.gke.io/managed-certificates"]).toBe("relaycast");
    expect(values.ingress.hosts).toEqual(["relay.gcp.example.com"]);
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
    expect(GCP_RUNTIME_SHAPE.natIpAllocateOption).toBe("MANUAL_ONLY");
    expect(GCP_RUNTIME_SHAPE.natLogFilter).toBe("ALL");
    expect(GCP_NODE_ROLES).not.toContain("roles/editor");
    expect(GCP_NODE_ROLES).not.toContain("roles/owner");
    const stack = readFileSync(new URL("../alchemy.gcp.run.ts", import.meta.url), "utf8");
    expect(stack).toContain("enablePrivateEndpoint: GCP_RUNTIME_SHAPE.privateEndpoint");
    expect(stack).toContain("autopilot: GCP_RUNTIME_SHAPE.autopilot");
    expect(stack).toContain("serviceAccount: nodeIdentity.email");
    expect(stack).toContain("natIpAllocateOption: GCP_RUNTIME_SHAPE.natIpAllocateOption");
    expect(stack).toContain("filter: GCP_RUNTIME_SHAPE.natLogFilter");
    expect(stack).toContain("purpose: \"VPC_PEERING\"");
    expect(stack).not.toContain("AUTO_ONLY");
    expect(stack).not.toContain("enablePrivateEndpoint: false");
    expect(stack).not.toContain("Shared VPC");
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
