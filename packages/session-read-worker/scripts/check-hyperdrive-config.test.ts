import { describe, expect, test } from "vitest";
import { checkHyperdriveConfig, EXPECTED_SUPABASE_HOST, EXPECTED_SUPABASE_USER } from "./hyperdrive-config.ts";

const expected = { id: "a".repeat(32), caId: "ca-certificate-id" };
const valid = () => ({
  success: true,
  result: {
    id: expected.id,
    caching: { disabled: true },
    mtls: { sslmode: "verify-full", ca_certificate_id: expected.caId },
    origin: { user: EXPECTED_SUPABASE_USER, host: EXPECTED_SUPABASE_HOST },
  },
});

describe("live Hyperdrive deploy guard", () => {
  test("accepts only the pinned read-role configuration", () => {
    const report = checkHyperdriveConfig(valid(), expected);
    expect(report.ok).toBe(true);
    expect(Object.values(report.checks).every(Boolean)).toBe(true);
  });

  test("mutation: enabling caching blocks the deploy", () => {
    const body = valid();
    body.result.caching.disabled = false;
    const report = checkHyperdriveConfig(body, expected);
    expect(report.ok).toBe(false);
    expect(report.checks["caching.disabled"]).toBe(false);
  });

  const mutations: Array<[string, (body: ReturnType<typeof valid>) => void]> = [
    ["mtls.sslmode", (body: ReturnType<typeof valid>) => (body.result.mtls.sslmode = "require")],
    ["mtls.ca_certificate_id", (body: ReturnType<typeof valid>) => (body.result.mtls.ca_certificate_id = "another-ca")],
    ["origin.user", (body: ReturnType<typeof valid>) => (body.result.origin.user = "postgres")],
    ["origin.host", (body: ReturnType<typeof valid>) => (body.result.origin.host = "another.pooler.supabase.com")],
  ];
  test.each(mutations)("refuses a changed %s pin", (field, mutate) => {
    const body = valid();
    mutate(body);
    const report = checkHyperdriveConfig(body, expected);
    expect(report.ok).toBe(false);
    expect(report.checks[field]).toBe(false);
  });
});
