import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { eksChartValues } from "../aws/eks.ts";
import {
  claimlessWindowEnv,
  type ClaimlessWindow,
} from "../claimless-window.ts";
import {
  bulls96ReleaseValues,
  GCP_APPROVED_SERVICES_IMAGE,
} from "../gcp/config.ts";
import { localClaimEnv } from "../local/topology.ts";

describe("claimlessWindowEnv", () => {
  const from = "2026-11-01T00:00:00.000Z";

  test("a stage that names no window sets nothing", () => {
    expect(claimlessWindowEnv(undefined)).toEqual([]);
  });

  test("a window sets all three variables", () => {
    const until = "2026-11-04T00:00:00.000Z";
    expect(claimlessWindowEnv({ from, until })).toEqual([
      { name: "MANIFEST_MACHINE_CLAIM", value: "compat" },
      { name: "MANIFEST_MACHINE_CLAIM_COMPAT_FROM", value: from },
      { name: "MANIFEST_MACHINE_CLAIM_COMPAT_UNTIL", value: until },
    ]);
  });

  test("refuses over the cap, backwards and unreadable windows", () => {
    for (const until of [
      "2026-11-04T00:00:00.001Z",
      from,
      "2026-10-31T00:00:00.000Z",
      "soon",
    ])
      expect(() => claimlessWindowEnv({ from, until })).toThrow(/72 hours/);
  });
});

const NAMES = [
  "MANIFEST_MACHINE_CLAIM",
  "MANIFEST_MACHINE_CLAIM_COMPAT_FROM",
  "MANIFEST_MACHINE_CLAIM_COMPAT_UNTIL",
];

type Rendered = { alleyoop: { extraEnv: { name: string; value: string }[] } };

const compositions: [string, (window?: ClaimlessWindow) => Rendered][] = [
  [
    "AWS",
    (claimlessWindow) =>
      eksChartValues(
        {
          stage: "aws-sessions-dev",
          runtimeSecretName: "96bulls-runtime",
          lockerBucket: "96bulls-locker",
          lockerRegion: "us-west-2",
          tenantOrgId: "org_sessions_dev",
          relayhistoryOrigin: "https://sessions-dev.agentrelay.com",
          imageRepository:
            "500268966557.dkr.ecr.us-west-2.amazonaws.com/96bulls-services",
          imageDigest: `sha256:${"a".repeat(64)}`,
          ...(claimlessWindow ? { claimlessWindow } : {}),
        },
        false,
        "http://relayauth.relayauth.svc:8787",
        "http://relayauth.relayauth.svc:8787/.well-known/jwks.json",
      ) as unknown as Rendered,
  ],
  [
    "GCP",
    (claimlessWindow) =>
      bulls96ReleaseValues(
        {
          enabled: true,
          chartVersion: "0.3.2",
          tenantOrgId: "org_123",
          existingSecretName: "bulls96-runtime",
          hoopdbRuntimeRole: "bulls96_runtime",
          hoopdbRuntimeRoleLogin: true,
          imageDigest: `sha256:${"a".repeat(64)}`,
          imageSourceRevision: GCP_APPROVED_SERVICES_IMAGE.sourceRevision,
          relayauthJwksUrl:
            "http://relayauth.relayauth.svc:8787/.well-known/jwks.json",
          relayauthIssuer: "http://relayauth.relayauth.svc:8787",
          relayhistoryOrigin: "http://bulls96-alleyoop.96bulls.svc:8080",
          ...(claimlessWindow ? { claimlessWindow } : {}),
        },
        {
          bucket: "locker",
          region: "us-central1",
          imageRepository:
            "us-central1-docker.pkg.dev/agentworkforce-dev1/awf-dev-images/96bulls",
          cloudSqlConnectionName:
            "agentworkforce-dev1:us-central1:awf-dev-postgres",
          redisHost: "10.52.0.3",
          redisPort: 6379,
          redisCaPem:
            "-----BEGIN CERTIFICATE-----\nQUJDRA==\n-----END CERTIFICATE-----\n",
          pubsubTopic: "projects/agentworkforce-dev1/topics/events",
          pubsubSubscription:
            "projects/agentworkforce-dev1/subscriptions/events",
          workloadIdentityEmails: {
            worker: "awf-dev-worker@agentworkforce-dev1.iam.gserviceaccount.com",
            alleyoop: "awf-dev-alleyoop@agentworkforce-dev1.iam.gserviceaccount.com",
          },
        },
      ) as unknown as Rendered,
  ],
];

describe("the local composition", () => {
  const from = "2026-11-01T00:00:00.000Z";
  const until = "2026-11-04T00:00:00.000Z";

  test("is off by default", () => {
    expect(localClaimEnv(undefined)).toEqual({});
  });

  test("propagates a configured window to the ingest", () => {
    expect(localClaimEnv({ from, until })).toEqual({
      MANIFEST_MACHINE_CLAIM: "compat",
      MANIFEST_MACHINE_CLAIM_COMPAT_FROM: from,
      MANIFEST_MACHINE_CLAIM_COMPAT_UNTIL: until,
    });
    expect(() =>
      localClaimEnv({ from, until: "2026-11-05T00:00:00.000Z" }),
    ).toThrow(/72 hours/);
  });

  test("alchemy.run.ts composes the ingest through it", () => {
    const stack = readFileSync(
      new URL("../../alchemy.run.ts", import.meta.url),
      "utf8",
    );
    expect(stack).toContain("...localClaimEnv(inputs.claimlessWindow)");
  });
});

describe.each(compositions)("the rendered %s composition", (_name, render) => {
  const from = "2026-11-01T00:00:00.000Z";
  const until = "2026-11-04T00:00:00.000Z";
  const claimEnv = (values: Rendered) =>
    values.alleyoop.extraEnv.filter((entry) => NAMES.includes(entry.name));

  test("is off by default", () => {
    expect(claimEnv(render())).toEqual([]);
    expect(JSON.stringify(render())).not.toContain("MANIFEST_MACHINE_CLAIM");
  });

  test("renders FROM and UNTIL together, only on the ingest", () => {
    const values = render({ from, until });
    expect(claimEnv(values)).toEqual([
      { name: "MANIFEST_MACHINE_CLAIM", value: "compat" },
      { name: "MANIFEST_MACHINE_CLAIM_COMPAT_FROM", value: from },
      { name: "MANIFEST_MACHINE_CLAIM_COMPAT_UNTIL", value: until },
    ]);
    expect(
      JSON.stringify(values).match(/MANIFEST_MACHINE_CLAIM"/g),
    ).toHaveLength(1);
  });

  test("refuses to render half a window, or one over the cap", () => {
    expect(() => render({ from, until: "" })).toThrow(/72 hours/);
    expect(() => render({ from: "", until })).toThrow(/72 hours/);
    expect(() => render({ from, until: "2026-11-04T00:00:00.001Z" })).toThrow(
      /72 hours/,
    );
  });
});
