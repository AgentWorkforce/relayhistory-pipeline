import { describe, expect, it } from "bun:test";
import { OTEL_ENDPOINT, OTEL_SERVICE_ACCOUNT, eksChartValues, telemetryPolicy } from "../eks.ts";

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

  it("archives lossless OTLP traces under the encrypted telemetry prefix", () => {
    const exporter = values.otel.config.exporters["awss3/traces"];
    expect(exporter.s3uploader).toMatchObject({
      region: "us-west-2",
      s3_bucket: "96bulls-locker",
      s3_base_prefix: "telemetry",
      s3_prefix: "traces",
    });
    expect(exporter.marshaler).toBe("otlp_json");
    expect(values.otel.config.service.pipelines.traces).toEqual({
      receivers: ["otlp"],
      processors: ["memory_limiter", "attributes/drop_forbidden", "redaction", "batch"],
      exporters: ["awss3/traces"],
    });
    expect(JSON.stringify(values.otel.config)).not.toContain("awsxray");
  });

  it("redacts secret-shaped values and drops content-bearing attributes before persistence", () => {
    expect(values.otel.config.processors.redaction.allow_all_keys).toBe(true);
    expect(values.otel.config.processors.redaction.blocked_values).toContain("AKIA[0-9A-Z]{16}");
    expect(values.otel.config.processors["attributes/drop_forbidden"].actions.map((a) => a.key).sort()).toEqual(
      ["body", "message", "path", "prompt", "summary"],
    );
  });

  it("does not place runtime secrets or static AWS credentials in chart values", () => {
    const rendered = JSON.stringify(values);
    expect(rendered).not.toMatch(/access.?key|secret.?access|summary-master-keys/i);
    expect(values.worker.locker.credentials.mode).toBe("podIdentity");
    expect(values.alleyoop.locker.credentials.mode).toBe("podIdentity");
  });

  it("gives the collector write-only access to telemetry objects", () => {
    const policy = telemetryPolicy({ lockerBucketArn: "arn:aws:s3:::locker", lockerKmsKeyArn: "arn:aws:kms:us-west-2:1:key/k" });
    const actions = policy.Statement.flatMap((s) => s.Action);
    expect(actions).toContain("s3:PutObject");
    expect(actions).toContain("kms:Decrypt");
    expect(actions).not.toContain("s3:GetObject");
    expect(actions).not.toContain("s3:DeleteObject");
    expect(policy.Statement[0]!.Resource).toEqual(["arn:aws:s3:::locker/telemetry/*"]);
  });
});
