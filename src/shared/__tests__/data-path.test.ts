import { describe, expect, test } from "bun:test";
import { dataPathVerdict, workloadReady } from "../data-path.ts";

const armed = {
  WORKER_DATA_PATH_PROBE: "1",
  LOCKER_PROVIDER: "gcs",
  LOCKER_BUCKET: "awf-dev-locker",
  PIPELINE_RAW_PROVIDER: "gcs",
  CLOUD_SQL_CONNECTION_NAME: "agentworkforce-dev1:us-central1:awf-dev-postgres",
  REDIS_HOST: "10.52.0.3",
  REDIS_PORT: "6379",
  PUBSUB_TOPIC: "projects/agentworkforce-dev1/topics/awf-dev-events",
  PUBSUB_SUBSCRIPTION: "projects/agentworkforce-dev1/subscriptions/awf-dev-events-worker",
};

describe("data-path readiness", () => {
  test("stays unarmed for the S3 default", () => {
    expect(dataPathVerdict({}).armed).toBe(false);
    expect(dataPathVerdict({ LOCKER_PROVIDER: "s3", LOCKER_BUCKET: "locker" }).ok).toBe(true);
    expect(workloadReady(true, {})).toEqual({ ok: true });
    expect(workloadReady(false, {})).toEqual({ ok: false });
  });

  test("requires the GCS data path and does not echo secret values", () => {
    expect(dataPathVerdict(armed)).toEqual({ ok: true, armed: true });
    const hmac = dataPathVerdict({ ...armed, LOCKER_ACCESS_KEY_ID: "AKIA_BOUNDARY_KEY", LOCKER_SECRET_ACCESS_KEY: "SECRET_BOUNDARY_VALUE" });
    expect(hmac.ok).toBe(false);
    if (!hmac.ok) expect(hmac.missing).toContain("LOCKER_HMAC");
    expect(JSON.stringify(hmac)).not.toContain("AKIA_BOUNDARY_KEY");
    expect(JSON.stringify(hmac)).not.toContain("SECRET_BOUNDARY_VALUE");
    const pipeline = dataPathVerdict({
      ...armed,
      PIPELINE_RAW_PROVIDER: "s3",
      PIPELINE_RAW_S3_ACCESS_KEY_ID: "AKIA_PIPELINE_KEY",
      PIPELINE_RAW_S3_SECRET_ACCESS_KEY: "PIPELINE_SECRET_VALUE",
    });
    expect(pipeline.ok).toBe(false);
    if (!pipeline.ok) {
      expect(pipeline.missing).toContain("PIPELINE_RAW_PROVIDER");
      expect(pipeline.missing).toContain("PIPELINE_HMAC");
    }
    expect(JSON.stringify(pipeline)).not.toContain("AKIA_PIPELINE_KEY");
    expect(JSON.stringify(pipeline)).not.toContain("PIPELINE_SECRET_VALUE");

    const dsn = "postgres://hoop:super-secret@127.0.0.1/hoopdb";
    const sql = dataPathVerdict({ ...armed, CLOUD_SQL_CONNECTION_NAME: dsn, REDIS_PASSWORD: "redis-secret" });
    expect(sql.ok).toBe(false);
    if (!sql.ok) {
      expect(sql.missing).toContain("CLOUD_SQL_CONNECTION_NAME");
      expect(sql.missing).toContain("REDIS_SECRET");
    }
    expect(JSON.stringify(sql)).not.toContain("super-secret");
    expect(JSON.stringify(sql)).not.toContain("redis-secret");
    expect(workloadReady(true, { ...armed, LOCKER_BUCKET: "  " })).toEqual({ ok: false, missing: ["LOCKER_BUCKET"] });
  });
});
