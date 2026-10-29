import { describe, expect, it } from "bun:test";
import { OTEL_ENDPOINT, OTEL_SERVICE_ACCOUNT, eksChartValues } from "../eks.ts";

const values = eksChartValues(
  {
    stage: "aws-sessions-dev",
    runtimeSecretName: "96bulls-runtime",
    lockerBucket: "96bulls-locker",
    lockerRegion: "us-west-2",
    tenantOrgId: "org_sessions_dev",
    relayhistoryOrigin: "https://sessions-dev.agentrelay.com",
    imageRepository: "500268966557.dkr.ecr.us-west-2.amazonaws.com/96bulls-services",
    imageDigest: `sha256:${"a".repeat(64)}`,
  },
  false,
  "http://relayauth.relayauth.svc:8787",
  "http://relayauth.relayauth.svc:8787/.well-known/jwks.json",
);

describe("AWS observability values", () => {
  it("sends every workload to the in-cluster collector", () => {
    expect(values.global.otel.endpoint).toBe(OTEL_ENDPOINT);
    expect(values.otel.enabled).toBe(true);
    expect(values.otel.fullnameOverride).toBe(OTEL_SERVICE_ACCOUNT);
  });

  it("exports traces to X-Ray in the environment region", () => {
    expect(values.otel.config.exporters.awsxray.region).toBe("us-west-2");
    expect(values.otel.config.service.pipelines.traces).toEqual({
      receivers: ["otlp"],
      processors: ["memory_limiter", "batch"],
      exporters: ["awsxray"],
    });
    expect(values.otel.config.exporters.awsxray.indexed_attributes).toContain("session_uuid");
    expect(values.otel.config.exporters.awsxray.indexed_attributes).toContain("stage");
  });

  it("does not place runtime secrets or static AWS credentials in chart values", () => {
    const rendered = JSON.stringify(values);
    expect(rendered).not.toMatch(/access.?key|secret.?access|summary-master-keys/i);
    expect(values.worker.locker.credentials.mode).toBe("podIdentity");
    expect(values.alleyoop.locker.credentials.mode).toBe("podIdentity");
  });
});
