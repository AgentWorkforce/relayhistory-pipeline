import { describe, expect, test } from "vitest";
import {
  checkHyperdriveConfig,
  EXPECTED_ANALYSIS_ADMIT_USER,
  parseHyperdriveRole,
  EXPECTED_SUPABASE_DATABASE,
  EXPECTED_SUPABASE_HOST,
  EXPECTED_SUPABASE_PORT,
  EXPECTED_SUPABASE_SCHEME,
  EXPECTED_SUPABASE_USER,
} from "./hyperdrive-config.ts";

const expected = { id: "a".repeat(32), caId: "ca-certificate-id" };
const valid = () => ({
  success: true,
  result: {
    id: expected.id,
    caching: { disabled: true },
    mtls: { sslmode: "verify-full", ca_certificate_id: expected.caId },
    origin: {
      scheme: EXPECTED_SUPABASE_SCHEME,
      host: EXPECTED_SUPABASE_HOST,
      port: EXPECTED_SUPABASE_PORT,
      database: EXPECTED_SUPABASE_DATABASE,
      user: EXPECTED_SUPABASE_USER,
    },
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
    ["origin.user", (body: ReturnType<typeof valid>) => (body.result.origin.user = "bulls96_read_api.anotherproject")],
    ["origin.host", (body: ReturnType<typeof valid>) => (body.result.origin.host = "another.pooler.supabase.com")],
    ["origin.scheme", (body: ReturnType<typeof valid>) => (body.result.origin.scheme = "mysql")],
    ["origin.port", (body: ReturnType<typeof valid>) => (body.result.origin.port = 6543)],
    ["origin.database", (body: ReturnType<typeof valid>) => (body.result.origin.database = "another_database")],
  ];
  test.each(mutations)("refuses a changed %s pin", (field, mutate) => {
    const body = valid();
    mutate(body);
    const report = checkHyperdriveConfig(body, expected);
    expect(report.ok).toBe(false);
    expect(report.checks[field]).toBe(false);
  });

  describe("the analysis admission role (HYPERDRIVE_ROLE=analysis_admit)", () => {
    const admit = { ...expected, role: "analysis_admit" as const };
    const admitValid = () => {
      const body = valid();
      body.result.origin.user = EXPECTED_ANALYSIS_ADMIT_USER;
      return body;
    };

    test("the admission user is the read role's pooler user with only the role name changed", () => {
      expect(EXPECTED_ANALYSIS_ADMIT_USER.startsWith("bulls96_analysis_admit.")).toBe(true);
      expect(EXPECTED_ANALYSIS_ADMIT_USER.slice("bulls96_analysis_admit.".length)).toBe(EXPECTED_SUPABASE_USER.slice("bulls96_read_api.".length));
    });

    test("accepts the pinned admission-role configuration", () => {
      expect(checkHyperdriveConfig(admitValid(), admit).ok).toBe(true);
    });

    test("caching must be disabled for the admission role too", () => {
      for (const caching of [{ disabled: false }, {}, undefined]) {
        const body = admitValid() as Record<string, any>;
        body.result.caching = caching;
        const report = checkHyperdriveConfig(body, admit);
        expect(report.ok).toBe(false);
        expect(report.checks["caching.disabled"]).toBe(false);
      }
    });

    test("the roles never cross: the read user fails the admission check and vice versa", () => {
      expect(checkHyperdriveConfig(valid(), admit).checks["origin.user"]).toBe(false);
      expect(checkHyperdriveConfig(admitValid(), expected).checks["origin.user"]).toBe(false);
      expect(checkHyperdriveConfig(admitValid(), { ...expected, role: "read_api" }).ok).toBe(false);
    });

    test.each(mutations)("refuses a changed %s pin", (field, mutate) => {
      const body = admitValid();
      mutate(body);
      if (field === "origin.user") body.result.origin.user = "bulls96_analysis_admit.anotherproject";
      const report = checkHyperdriveConfig(body, admit);
      expect(report.ok).toBe(false);
      expect(report.checks[field]).toBe(false);
    });

    test("HYPERDRIVE_ROLE: unset/empty = read_api; only the two role names are accepted", () => {
      for (const raw of [undefined, "", " "]) expect(parseHyperdriveRole(raw)).toBe("read_api");
      expect(parseHyperdriveRole("analysis_admit")).toBe("analysis_admit");
      for (const raw of ["admit", "ANALYSIS_ADMIT", "service_role", "read-api"]) expect(() => parseHyperdriveRole(raw)).toThrow(/HYPERDRIVE_ROLE/);
    });
  });
});
