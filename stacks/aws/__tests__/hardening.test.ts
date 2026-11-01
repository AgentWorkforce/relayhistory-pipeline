import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { manifestKey, rawChunkKey, type SessionLocation } from "../../../packages/object-store/src/layout.ts";
import { summaryObjectKey } from "../../../src/bob-costas/summary-objects.ts";
import {
  CONTROL_PLANE_LOG_RETENTION,
  CONTROL_PLANE_LOG_TYPES,
  PINNED_IMAGES,
  SERVICE_ACCOUNTS,
  assertDigestPinned,
  assertServicesImage,
  clusterEndpointAccess,
  controlPlaneLogGroupName,
  controlPlaneLogging,
  hoopdbBackend,
  denyInsecureTransport,
  ecrPushPolicy,
  githubImagePublishTrust,
  lockerLifecycleRules,
  podIdentityTrust,
  podRoles,
  trailKeyPolicy,
} from "../hardening.ts";
import { eksChartValues } from "../eks.ts";

const ROOT = fileURLToPath(new URL("../../..", import.meta.url));
const refs = { lockerBucketArn: "arn:aws:s3:::locker", lockerKmsKeyArn: "arn:aws:kms:us-west-2:1:key/k", lockerRegion: "us-west-2" };
const DIGEST = `sha256:${"a".repeat(64)}`;

type Stmt = { Sid?: string; Effect: string; Action: string[]; Resource: string[]; Condition?: unknown };
const statements = (sa: string) => (podRoles(refs).find((r) => r.serviceAccount === sa)!.policy.Statement as Stmt[]);
const allowed = (sa: string, action: string) =>
  statements(sa).filter((s) => s.Effect === "Allow" && s.Action.includes(action)).flatMap((s) => s.Resource);
/** IAM resource glob -> RegExp (`*` matches across `/`, as in IAM). */
const glob = (pattern: string) => new RegExp(`^${pattern.replace(/[.+?^${}()|[\]\\]/g, "\\$&").replace(/\*/g, ".*")}$`);
const grants = (sa: string, action: string, objectKey: string) => allowed(sa, action).some((r) => glob(r).test(`${refs.lockerBucketArn}/${objectKey}`));

describe("EKS API endpoint", () => {
  it("is private only by default: no public endpoint, no CIDR list", () => {
    expect(clusterEndpointAccess(undefined)).toEqual({ endpointPrivateAccess: true, endpointPublicAccess: false });
    expect(clusterEndpointAccess([])).toEqual({ endpointPrivateAccess: true, endpointPublicAccess: false });
  });

  it("opens the public endpoint only for listed operator CIDRs", () => {
    expect(clusterEndpointAccess(["203.0.113.7/32", "198.51.100.0/24"])).toEqual({
      endpointPrivateAccess: true,
      endpointPublicAccess: true,
      publicAccessCidrs: ["198.51.100.0/24", "203.0.113.7/32"],
    });
  });

  it("refuses 0.0.0.0/0, ranges broader than /16, malformed and duplicate entries", () => {
    for (const bad of [["0.0.0.0/0"], ["10.0.0.0/8"], ["203.0.113.7"], ["300.1.1.1/32"], ["::/0"], ["1.2.3.4/32", "1.2.3.4/32"]])
      expect(() => clusterEndpointAccess(bad)).toThrow(/Nothing was created/);
  });
});

describe("control-plane logging", () => {
  it("enables all five log types with a bounded retention on the EKS log group", () => {
    expect(controlPlaneLogging()).toEqual({ clusterLogging: [{ types: ["api", "audit", "authenticator", "controllerManager", "scheduler"], enabled: true }] });
    expect(CONTROL_PLANE_LOG_TYPES).toHaveLength(5);
    expect(CONTROL_PLANE_LOG_RETENTION).toBe("90 days");
    expect(controlPlaneLogGroupName("96bulls-aws-sessions-dev")).toBe("/aws/eks/96bulls-aws-sessions-dev/cluster");
  });
});

describe("per-service-account pod roles", () => {
  const roles = podRoles(refs);
  const loc: SessionLocation = { workspaceId: "ws1", machineId: "m1", harness: "claude", startDate: "2026-10-30", sessionId: "s1" };
  const raw = rawChunkKey(loc, 1, 0, "ab".repeat(32), "jsonl");
  const manifest = manifestKey(loc);
  const summary = summaryObjectKey(loc, "short");

  it("declares one role per service account and shares none", () => {
    expect(roles.map((r) => r.serviceAccount).sort()).toEqual(Object.values(SERVICE_ACCOUNTS).sort());
    expect(new Set(roles.map((r) => r.id)).size).toBe(roles.length);
    // The old shared role survives only as an unattached rollback copy behind `legacyPodsRole: "keep"`.
    const eks = readFileSync(`${ROOT}/stacks/aws/eks.ts`, "utf8");
    expect(eks.match(/"pods-role"/g)).toHaveLength(1);
    expect(eks).toContain('if (i.legacyPodsRole === "keep") yield* AWS.IAM.Role("pods-role", legacyPodsRoleProps(i));');
    expect(eks).not.toMatch(/for \(const sa of \[/);
    expect(eks).not.toMatch(/podRole\.roleArn/);
  });

  it("grants s3:DeleteObject only to AlleyOop, only for parked-session cleanup", () => {
    for (const r of roles)
      for (const s of r.policy.Statement as Stmt[]) {
        if (s.Effect !== "Allow") continue;
        expect(s.Action.some((a) => /s3:\*|DeleteObjectVersion|DeleteBucket/.test(a))).toBe(false);
        if (s.Action.includes("s3:DeleteObject")) {
          expect(r.serviceAccount).toBe(SERVICE_ACCOUNTS.alleyoop);
          expect(s.Action).toEqual(["s3:DeleteObject"]);
        }
      }
    const parked = manifest.replace(/manifest\.json$/, "parked/box-1/manifest.json");
    expect(grants(SERVICE_ACCOUNTS.alleyoop, "s3:DeleteObject", parked)).toBe(true);
    expect(grants(SERVICE_ACCOUNTS.alleyoop, "s3:DeleteObject", raw)).toBe(true);
    expect(grants(SERVICE_ACCOUNTS.alleyoop, "s3:PutObject", parked)).toBe(true);
    // A live manifest or a summary is never deletable by a pod.
    expect(grants(SERVICE_ACCOUNTS.alleyoop, "s3:DeleteObject", manifest)).toBe(false);
    expect(grants(SERVICE_ACCOUNTS.alleyoop, "s3:DeleteObject", summary)).toBe(false);
    for (const key of [manifest, raw, summary, parked])
      expect(grants(SERVICE_ACCOUNTS.worker, "s3:DeleteObject", key)).toBe(false);
  });

  it("lets the worker read session objects and write only encrypted summaries (keys from the real layout)", () => {
    expect(grants(SERVICE_ACCOUNTS.worker, "s3:GetObject", manifest)).toBe(true);
    expect(grants(SERVICE_ACCOUNTS.worker, "s3:GetObject", raw)).toBe(true);
    expect(grants(SERVICE_ACCOUNTS.worker, "s3:PutObject", summary)).toBe(true);
    expect(grants(SERVICE_ACCOUNTS.worker, "s3:PutObject", raw)).toBe(false);
    expect(grants(SERVICE_ACCOUNTS.worker, "s3:PutObject", manifest)).toBe(false);
    expect(grants(SERVICE_ACCOUNTS.worker, "s3:GetObject", "telemetry/traces/x.json.gz")).toBe(false);
  });

  it("lets AlleyOop write raw chunks and manifests, not summaries or telemetry", () => {
    expect(grants(SERVICE_ACCOUNTS.alleyoop, "s3:PutObject", raw)).toBe(true);
    expect(grants(SERVICE_ACCOUNTS.alleyoop, "s3:PutObject", manifest)).toBe(true);
    expect(grants(SERVICE_ACCOUNTS.alleyoop, "s3:PutObject", summary)).toBe(false);
    expect(grants(SERVICE_ACCOUNTS.alleyoop, "s3:GetObject", "telemetry/traces/x.json.gz")).toBe(false);
  });

  it("keeps KMS to data keys and decrypt through S3; no kms:Encrypt for the apps", () => {
    for (const sa of [SERVICE_ACCOUNTS.worker, SERVICE_ACCOUNTS.alleyoop]) {
      const kms = statements(sa).filter((s) => s.Action.some((a) => a.startsWith("kms:")));
      expect(kms).toHaveLength(1);
      expect(kms[0]!.Action.sort()).toEqual(["kms:Decrypt", "kms:GenerateDataKey"]);
      expect(kms[0]!.Condition).toEqual({ StringEquals: { "kms:ViaService": "s3.us-west-2.amazonaws.com" } });
    }
  });

  it("gives the search worker an explicit role that can reach neither the Locker nor its key", () => {
    const s = statements(SERVICE_ACCOUNTS.searchWorker);
    expect(s.every((x) => x.Effect === "Deny")).toBe(true);
    expect(s.flatMap((x) => x.Action).sort()).toEqual(["kms:*", "s3:*"]);
    expect(s.flatMap((x) => x.Resource)).toEqual(["arn:aws:s3:::locker", "arn:aws:s3:::locker/*", refs.lockerKmsKeyArn]);
  });

  it("keeps the collector Put-only under telemetry/traces/", () => {
    expect(allowed(SERVICE_ACCOUNTS.otel, "s3:PutObject")).toEqual(["arn:aws:s3:::locker/telemetry/traces/*"]);
    expect(allowed(SERVICE_ACCOUNTS.otel, "s3:GetObject")).toEqual([]);
  });

  it("binds Pod Identity trust to this account and cluster", () => {
    const t = podIdentityTrust("500268966557", "arn:aws:eks:us-west-2:500268966557:cluster/96bulls-aws-sessions-dev");
    expect(t.Statement[0]!.Condition).toEqual({
      StringEquals: { "aws:SourceAccount": "500268966557" },
      ArnEquals: { "aws:SourceArn": "arn:aws:eks:us-west-2:500268966557:cluster/96bulls-aws-sessions-dev" },
    });
  });
});

describe("image provenance", () => {
  it("refuses tag-only and malformed references", () => {
    for (const bad of ["", "latest", "96bulls-services:sha-4703c8e", "ghcr.io/x/y:1.0", "sha256:abc", `sha256:${"A".repeat(64)}`])
      expect(() => assertDigestPinned("x", bad)).toThrow(/Nothing was created/);
    expect(assertDigestPinned("x", `ghcr.io/x/y@${DIGEST}`)).toBe(DIGEST);
  });

  it("requires the services image as a bare digest with its 40-hex source commit", () => {
    expect(() => assertServicesImage({ imageDigest: DIGEST, imageSourceRevision: "f".repeat(40) })).not.toThrow();
    expect(() => assertServicesImage({ imageDigest: DIGEST })).toThrow(/imageSourceRevision/);
    expect(() => assertServicesImage({ imageDigest: DIGEST, imageSourceRevision: "4703c8e" })).toThrow(/imageSourceRevision/);
    expect(() => assertServicesImage({ imageDigest: "96bulls-services:latest", imageSourceRevision: "f".repeat(40) })).toThrow(/tag-only/);
    expect(() => assertServicesImage({ imageDigest: `repo@${DIGEST}`, imageSourceRevision: "f".repeat(40) })).toThrow(/bare digest/);
  });

  it("pins every third-party image by digest, matching versions.lock.json", () => {
    const lock = JSON.parse(readFileSync(`${ROOT}/versions.lock.json`, "utf8")) as { images: Record<string, { ref: string; digest?: string }> };
    expect(lock.images["otel-collector"]!.digest).toBe(PINNED_IMAGES.otelCollector.digest);
    expect(lock.images["cnpg-operator"]!.digest).toBe(PINNED_IMAGES.cnpgOperator.digest);
    expect(lock.images["cnpg-postgres"]!.digest).toBe(PINNED_IMAGES.cnpgPostgres.digest);
    for (const img of Object.values(PINNED_IMAGES)) expect(img.digest).toMatch(/^sha256:[0-9a-f]{64}$/);
  });

  it("renders the collector by digest", () => {
    const v = eksChartValues(
      { stage: "aws-sessions-dev", runtimeSecretName: "rt", lockerBucket: "b", lockerRegion: "us-west-2", tenantOrgId: "o", relayhistoryOrigin: "https://x.example.com", imageRepository: "r", imageDigest: DIGEST },
      false, "https://iss", "https://iss/jwks",
    );
    expect(v.otel.image.digest).toBe(PINNED_IMAGES.otelCollector.digest);
    expect(v.worker.image.digest).toBe(DIGEST);
    expect(v.alleyoop.image.digest).toBe(DIGEST);
  });
});

describe("CloudTrail key", () => {
  const p = trailKeyPolicy("500268966557", "us-west-2", "96bulls-aws-sessions-dev-trail");
  it("lets the account administer the key (no lockout) but not use it", () => {
    const admin = p.Statement[0]!;
    expect(admin).toMatchObject({ Principal: { AWS: "arn:aws:iam::500268966557:root" } });
    expect(admin.Action).toContain("kms:Put*");
    expect(admin.Action).not.toContain("kms:*");
    for (const a of admin.Action as string[]) expect(a).not.toMatch(/Decrypt|Encrypt|GenerateDataKey|ReEncrypt/);
    expect(p.Statement.flatMap((s) => s.Action as string[])).not.toContain("kms:*");
  });
  it("lets CloudTrail generate data keys for this trail only", () => {
    const ct = p.Statement.filter((s) => (s.Principal as { Service?: string }).Service === "cloudtrail.amazonaws.com");
    expect(ct).toHaveLength(2);
    for (const s of ct) expect(s.Condition).toMatchObject({ StringEquals: { "aws:SourceArn": "arn:aws:cloudtrail:us-west-2:500268966557:trail/96bulls-aws-sessions-dev-trail" } });
  });
  it("grants no account-wide decrypt/re-encrypt and scopes an optional reader to one named principal", () => {
    const closed = trailKeyPolicy("500268966557", "us-west-2", "96bulls-aws-sessions-dev-trail", null);
    expect(JSON.stringify(closed)).not.toMatch(/kms:Decrypt|kms:ReEncrypt/);
    expect(JSON.stringify(closed)).not.toContain('"AWS":"*"');
    const audit = "arn:aws:iam::500268966557:role/security/trail-auditor";
    const withAudit = trailKeyPolicy("500268966557", "us-west-2", "96bulls-aws-sessions-dev-trail", audit);
    const read = withAudit.Statement.find((s) => s.Sid === "AccountReadsLogs")!;
    expect(read.Principal).toEqual({ AWS: audit });
  });
});

describe("HoopDb backend", () => {
  it("defaults to external (Supabase) and allows CNPG only on proof and sandbox stages", () => {
    expect(hoopdbBackend("aws-sessions-dev", undefined)).toBe("external");
    expect(hoopdbBackend("aws-proof", "cnpg")).toBe("cnpg");
    expect(hoopdbBackend("aws-sandbox-claude", "cnpg")).toBe("cnpg");
    // sessions-dev must never be switched to an empty in-cluster database.
    expect(() => hoopdbBackend("aws-sessions-dev", "cnpg")).toThrow(/keeps its external HoopDb/);
  });
  it("refuses CNPG on every production-looking stage before the proof/sandbox allowlist", () => {
    for (const stage of ["aws-proof-prod", "aws-production", "aws-sandbox-prod-agent", "aws-proof-preprod", "aws-PROD"])
      expect(() => hoopdbBackend(stage, "cnpg")).toThrow(/looks like production/);
  });

  it("refuses in-cluster CNPG for any production-looking stage, and unknown backends", () => {
    expect(() => hoopdbBackend("aws-prod", "cnpg")).toThrow(/Supabase/);
    expect(() => hoopdbBackend("aws-sessions-prod", "cnpg")).toThrow(/Supabase/);
    expect(() => hoopdbBackend("aws-sessions-dev", "aurora")).toThrow(/Nothing was created/);
  });
});

describe("buckets", () => {
  it("deny plain-HTTP access to the bucket and its objects", () => {
    expect(denyInsecureTransport("arn:aws:s3:::b")).toEqual([
      { Sid: "DenyInsecureTransport", Effect: "Deny", Principal: "*", Action: ["s3:*"], Resource: ["arn:aws:s3:::b", "arn:aws:s3:::b/*"], Condition: { Bool: { "aws:SecureTransport": "false" } } },
    ]);
    const stack = readFileSync(`${ROOT}/stacks/aws/alchemy.aws.run.ts`, "utf8");
    expect(stack.match(/denyInsecureTransport\(/g)?.length).toBe(2); // locker + trail
    expect(readFileSync(`${ROOT}/stacks/aws/ingress.ts`, "utf8")).toContain("denyInsecureTransport(logBucketArn)");
  });

  it("expires superseded Locker versions only when a stage opts in (irreversible), never current objects", () => {
    expect(lockerLifecycleRules(undefined).map((r) => r.ID)).toEqual(["abort-incomplete-uploads"]);
    const rules = lockerLifecycleRules(90);
    expect(rules.map((r) => r.ID)).toEqual(["expire-noncurrent-versions", "abort-incomplete-uploads"]);
    expect(JSON.stringify(rules)).not.toMatch(/"Expiration"/);
    expect(() => lockerLifecycleRules(7)).toThrow(/at least 30/);
  });

  it("encrypt the trail bucket with the trail key and keep it versioned", () => {
    const stack = readFileSync(`${ROOT}/stacks/aws/alchemy.aws.run.ts`, "utf8");
    const trail = stack.slice(stack.indexOf('AWS.S3.Bucket("trail-logs"'), stack.indexOf('AWS.CloudTrail.Trail("trail"'));
    expect(trail).toContain('versioning: "Enabled"');
    expect(trail).toContain('sseAlgorithm: "aws:kms", kmsMasterKeyId: trailKey.keyArn');
  });
});

describe("GitHub image publisher", () => {
  it("can push to the services ECR repository and nothing else", () => {
    const p = ecrPushPolicy("us-west-2", "500268966557");
    const actions = p.Statement.flatMap((s) => s.Action);
    expect(actions.every((a) => a.startsWith("ecr:"))).toBe(true);
    expect(p.Statement[1]!.Resource).toEqual(["arn:aws:ecr:us-west-2:500268966557:repository/96bulls-services"]);
    const stack = readFileSync(`${ROOT}/stacks/aws/alchemy.aws.run.ts`, "utf8");
    const deployer = stack.slice(stack.indexOf('AWS.IAM.Role("deployer"'), stack.indexOf("// HoopDb for this stage"));
    expect(deployer).not.toMatch(/s3:|kms:|locker:/);
  });

  it("is assumable only from this repository's image-publish environment", () => {
    const t = githubImagePublishTrust("arn:aws:iam::1:oidc-provider/token.actions.githubusercontent.com", "repo:AgentWorkforce@252050005/96BullSessions@1437714333");
    expect(t.Statement).toHaveLength(1);
    expect(t.Statement[0]!.Condition.StringEquals).toEqual({
      "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
      "token.actions.githubusercontent.com:sub": "repo:AgentWorkforce@252050005/96BullSessions@1437714333:environment:image-publish",
    });
  });
});

describe("sandbox permissions boundary", () => {
  const b = JSON.parse(readFileSync(`${ROOT}/stacks/aws/policies/sandbox-boundary.draft.json`, "utf8")) as { Statement: { Sid: string; Effect: string; Action: string[] }[] };
  const allows = b.Statement.filter((s) => s.Effect === "Allow").flatMap((s) => s.Action);
  const denies = b.Statement.filter((s) => s.Effect === "Deny").flatMap((s) => s.Action);
  it("allows neither role-trust edits nor new identity providers, and denies both explicitly", () => {
    for (const a of ["iam:UpdateAssumeRolePolicy", "iam:CreateOpenIDConnectProvider", "iam:CreateSAMLProvider"]) {
      expect(allows).not.toContain(a);
      expect(denies).toContain(a);
    }
  });
});

describe("collector pod", () => {
  it("mounts no Kubernetes API token and runs restricted", () => {
    const v = eksChartValues(
      { stage: "aws-sessions-dev", runtimeSecretName: "rt", lockerBucket: "b", lockerRegion: "us-west-2", tenantOrgId: "o", relayhistoryOrigin: "https://x.example.com", imageRepository: "r", imageDigest: DIGEST },
      false, "https://iss", "https://iss/jwks",
    );
    expect(v.otel.serviceAccount.automountServiceAccountToken).toBe(false);
    expect(v.otel.securityContext).toEqual({ allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: { drop: ["ALL"] } });
    expect(v.otel.podSecurityContext.runAsNonRoot).toBe(true);
  });
});

describe("Locker key layout the pod roles assume", () => {
  it("the AWS stage sets no deployment key prefix, so session keys start at v1/ (pod-role globs depend on it)", () => {
    const v = eksChartValues(
      { stage: "aws-proof", runtimeSecretName: "rt", lockerBucket: "b", lockerRegion: "us-west-2", tenantOrgId: "o", relayhistoryOrigin: "https://x.example.com", imageRepository: "r", imageDigest: DIGEST },
      false, "https://iss", "https://iss/jwks",
    );
    // E3 (#69) makes `<PIPELINE_KEY_PREFIX>/<org>/v1/...` possible; adopting it here means changing workerPolicy/alleyoopPolicy first.
    expect(JSON.stringify(v)).not.toMatch(/PIPELINE_KEY_PREFIX|PIPELINE_ORG_FROM_PREFIX/);
  });
});
