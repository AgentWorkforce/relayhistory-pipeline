import { describe, expect, test } from "bun:test";
import { GCP_CONFIRM_PROJECT_ENV } from "../config.ts";
import { assertGcpDeploymentTarget, assertGcpProjectConfirmation } from "../deploy-guard.ts";

describe("GCP deployment target guard", () => {
  const expected = { projectId: "agentworkforce-dev1", region: "us-central1" };

  test("accepts the live project in the approved region", () => {
    expect(() => assertGcpDeploymentTarget(expected, expected)).not.toThrow();
  });

  test("rejects a different live project", () => {
    expect(() =>
      assertGcpDeploymentTarget(expected, {
        projectId: "other-gcp-dev1",
        region: "us-central1",
      }),
    ).toThrow(/live GCP project.*Nothing was created/);
  });

  test("rejects a different resolved provider region", () => {
    expect(() =>
      assertGcpDeploymentTarget(expected, {
        projectId: "agentworkforce-dev1",
        region: "europe-west1",
      }),
    ).toThrow(/resolved GCP region.*Nothing was created/);
  });

  test("rejects a missing or different project confirmation", () => {
    expect(() => assertGcpProjectConfirmation(expected.projectId, undefined)).toThrow(
      new RegExp(`${GCP_CONFIRM_PROJECT_ENV}=${expected.projectId}.*Nothing was created`),
    );
    expect(() => assertGcpProjectConfirmation(expected.projectId, "other-gcp-dev1")).toThrow(/Nothing was created/);
  });

  test("accepts confirmation of the approved project only", () => {
    expect(() => assertGcpProjectConfirmation(expected.projectId, expected.projectId)).not.toThrow();
  });
});
