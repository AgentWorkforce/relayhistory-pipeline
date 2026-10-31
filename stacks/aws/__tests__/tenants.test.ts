import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { manifestKey, rawChunkKey, type SessionLocation } from "../../../packages/object-store/src/layout.ts";
import { summaryObjectKey } from "../../../src/bob-costas/summary-objects.ts";
import { prefixAllowed } from "../../../packages/pipeline/src/lib/pipeline/adapter.ts";
import { eksChartValues } from "../eks.ts";
import { SERVICE_ACCOUNTS, podRoles } from "../hardening.ts";
import { SESSION_DIRECTORY_POSTGREST } from "../proof-addons.ts";
import { RELAYAUTH_TLS_URL } from "../relayauth-tls.ts";
import { TENANT_KEY_BASE, resolveTenantOrgs, tenantWiring } from "../tenants.ts";

const DIGEST = `sha256:${"a".repeat(64)}`;
const base = { stage: "aws-proof", runtimeSecretName: "rt", lockerBucket: "b", lockerRegion: "us-west-2", relayhistoryOrigin: "https://proof-sessions.agentrelay.com", imageRepository: "r", imageDigest: DIGEST };
const env = (v: { extraEnv?: { name: string; value: string }[] }) => Object.fromEntries((v.extraEnv ?? []).map((e) => [e.name, e.value]));

describe("tenant orgs", () => {
  it("keeps the single-org shape (sessions-dev values change only by the explicit database-mode env)", () => {
    const v = eksChartValues({ ...base, tenantOrgId: "org_sessions_dev" }, false, "i", "j");
    expect(v.global.tenant.orgId).toBe("org_sessions_dev");
    // External (Supabase): supabase mode, the shim off, and no runtime role default, so the
    // migrator refuses to run until hoopdbRuntimeRole is set.
    expect(v.worker.extraEnv).toEqual([
      { name: "HOOPDB_DB_MODE", value: "supabase" },
      { name: "HOOPDB_SUPABASE_SHIM", value: "false" },
    ]);
    expect(v.alleyoop.extraEnv).toEqual([{ name: "RELAYHISTORY_ORIGIN", value: "https://proof-sessions.agentrelay.com" }]);
    expect(eksChartValues({ ...base, tenantOrgIds: ["org_sessions_dev"] }, false, "i", "j")).toEqual(v);
  });

  it("accepts two orgs: org-derived prefixes for both apps and an allowlist for AlleyOop", () => {
    const v = eksChartValues({ ...base, tenantOrgIds: ["org_aws_proof_1", "org_aws_proof_2"] }, false, "i", "j");
    expect(v.global.tenant.orgId).toBe("org_aws_proof_1");
    const want = { PIPELINE_ORG_FROM_PREFIX: "true", PIPELINE_KEY_PREFIX: TENANT_KEY_BASE, TENANT_ORG_IDS: "org_aws_proof_1,org_aws_proof_2" };
    expect(env(v.worker as never)).toEqual({ HOOPDB_DB_MODE: "supabase", HOOPDB_SUPABASE_SHIM: "false", ...want });
    expect(env(v.alleyoop)).toEqual({ RELAYHISTORY_ORIGIN: "https://proof-sessions.agentrelay.com", ...want });
  });

  it("validates the list and keeps tenantOrgId back-compatible", () => {
    expect(resolveTenantOrgs({ tenantOrgId: "org_a" })).toEqual(["org_a"]);
    expect(resolveTenantOrgs({ tenantOrgId: "org_a", tenantOrgIds: ["org_a", "org_b"] })).toEqual(["org_a", "org_b"]);
    expect(() => resolveTenantOrgs({ tenantOrgId: "org_x", tenantOrgIds: ["org_a", "org_b"] })).toThrow(/must equal eks.tenantOrgIds\[0\]/);
    expect(() => resolveTenantOrgs({ tenantOrgIds: ["org_a", "org_a"] })).toThrow(/duplicates/);
    expect(() => resolveTenantOrgs({ tenantOrgIds: [] })).toThrow(/required/);
    expect(() => resolveTenantOrgs({})).toThrow(/required/);
    expect(() => resolveTenantOrgs({ tenantOrgIds: ["org a"] })).toThrow(/not a valid org id/);
  });

  it("scopes the pod roles to the prefixed keys the pipeline actually writes for each org", () => {
    const { keyBase } = tenantWiring(["org_aws_proof_1", "org_aws_proof_2"]);
    const roles = podRoles({ lockerBucketArn: "arn:aws:s3:::l", lockerKmsKeyArn: "k", lockerRegion: "us-west-2", keyBase });
    const res = (sa: string, action: string) =>
      (roles.find((r) => r.serviceAccount === sa)!.policy.Statement as { Effect: string; Action: string[]; Resource: string[] }[])
        .filter((s) => s.Effect === "Allow" && s.Action.includes(action)).flatMap((s) => s.Resource);
    const glob = (p: string) => new RegExp(`^${p.replace(/[.+?^${}()|[\]\\]/g, "\\$&").replace(/\*/g, ".*")}$`);
    const can = (sa: string, action: string, key: string) => res(sa, action).some((r) => glob(r).test(`arn:aws:s3:::l/${key}`));
    for (const org of ["org_aws_proof_1", "org_aws_proof_2"]) {
      const loc: SessionLocation = { prefix: `${TENANT_KEY_BASE}/${org}`, workspaceId: "ws", machineId: "m", harness: "claude", startDate: "2026-10-31", sessionId: "s" };
      // The same `<base>/<org>` prefix the ingest applies and prefixAllowed accepts.
      expect(prefixAllowed({ PIPELINE_ORG_FROM_PREFIX: "true", PIPELINE_KEY_PREFIX: TENANT_KEY_BASE }, loc.prefix, org)).toBe(true);
      expect(can(SERVICE_ACCOUNTS.alleyoop, "s3:PutObject", rawChunkKey(loc, 0, 0, "ab".repeat(32), "jsonl"))).toBe(true);
      expect(can(SERVICE_ACCOUNTS.alleyoop, "s3:PutObject", manifestKey(loc))).toBe(true);
      expect(can(SERVICE_ACCOUNTS.worker, "s3:PutObject", summaryObjectKey(loc, "short"))).toBe(true);
      expect(can(SERVICE_ACCOUNTS.worker, "s3:GetObject", manifestKey(loc))).toBe(true);
      // Not outside the tenant tree.
      expect(can(SERVICE_ACCOUNTS.worker, "s3:GetObject", "telemetry/traces/x")).toBe(false);
      expect(can(SERVICE_ACCOUNTS.alleyoop, "s3:PutObject", manifestKey({ ...loc, prefix: undefined }))).toBe(false);
    }
  });
});

describe("harness endpoints", () => {
  it("names the RelayAuth TLS endpoint and the #73 PostgREST Service", () => {
    expect(RELAYAUTH_TLS_URL).toBe("https://relayauth-tls.relayauth.svc:8443");
    expect(SESSION_DIRECTORY_POSTGREST).toMatchObject({ service: "session-directory-postgrest", namespace: "96bulls", port: 3000, url: "http://session-directory-postgrest.96bulls.svc:3000" });
    const eks = readFileSync(fileURLToPath(new URL("../eks.ts", import.meta.url)), "utf8");
    expect(eks).toContain("relayauthTls: selfAuth");
    expect(eks).toContain('caConfigMap: { name: "relayauth-ca", key: "ca.crt", namespace: NAMESPACE, publishedBy: "operator" }');
    expect(eks).toContain("sessionDirectoryPostgrest: sessionDirectory");
  });
});
