import { describe, expect, it } from "bun:test";
import {
  assertGcpInputs,
  relayauthChartValues,
  relaycastChartValues,
  relayfileChartValues,
  relayflowsChartValues,
  type GcpInputs,
} from "../config.ts";

const base: GcpInputs = {
  projectId: "agentworkforce-dev1",
  projectNumber: "123456789012",
  region: "us-central1",
  controlPlaneAuthorizedCidrs: [{ cidrBlock: "203.0.113.10/32", displayName: "operator" }],
  clearedBy: "Khaliq: code review fixture only, 2026-10-29",
  managedDataPlane: { provision: false },
};

describe("GCP self-host contract", () => {
  it("accepts a substrate-only stage without pretending managed services are attached", () => {
    expect(() => assertGcpInputs("gcp-dev", base)).not.toThrow();
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

  it("fails closed when asked to install 96Bulls before GCS is wired into the services", () => {
    expect(() =>
      assertGcpInputs("gcp-dev", {
        ...base,
        bulls96: { enabled: true, chartVersion: "0.3.0" },
      }),
    ).toThrow("service Locker wiring is S3-only");
  });

  it("keeps Relaycast on the released single-writer SQLite profile", () => {
    const values = relaycastChartValues({
      chartVersion: "0.1.0",
      baseUrl: "https://relay.gcp.example.com",
    });
    expect(values.replicaCount).toBe(1);
    expect(values.runtime.backend).toBe("sqlite");
    expect(values.persistence.accessMode).toBe("ReadWriteOnce");
    expect(values.ingress.annotations["networking.gke.io/managed-certificates"]).toBe("relaycast");
    expect(values.ingress.hosts).toEqual(["relay.gcp.example.com"]);
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
