import { describe, expect, test } from "bun:test";
import {
  GCP_CONFIRM_PROJECT_ENV,
  GCP_CONFIRM_KUBERNETES_APPLY_ENV,
  GCP_CONFIRM_SQL_POST_ENV,
  assertGcpSqlPostConfirmation,
  assertGcpKubernetesApplyConfirmation,
} from "../config.ts";
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

  test("requires an exact, separate apply-time Cloud SQL mutation confirmation", () => {
    const expectedValue = "agentworkforce-dev1/awf-dev-postgres";
    expect(() => assertGcpSqlPostConfirmation("agentworkforce-dev1", "awf-dev-postgres", undefined)).toThrow(
      new RegExp(`${GCP_CONFIRM_SQL_POST_ENV}=${expectedValue}.*Nothing was created`),
    );
    expect(() =>
      assertGcpSqlPostConfirmation("agentworkforce-dev1", "awf-dev-postgres", "other/awf-dev-postgres"),
    ).toThrow(/Nothing was created/);
    expect(() =>
      assertGcpSqlPostConfirmation("agentworkforce-dev1", "awf-dev-postgres", expectedValue),
    ).not.toThrow();
  });

  test("requires the fresh apply id before the Kubernetes reachability action runs", () => {
    const applyId = "c05b704a-62d1-4d70-a1f9-a98c9e45bb25";
    expect(() => assertGcpKubernetesApplyConfirmation(applyId, undefined)).toThrow(
      new RegExp(`${GCP_CONFIRM_KUBERNETES_APPLY_ENV}=${applyId}.*No Kubernetes objects were applied`),
    );
    expect(() => assertGcpKubernetesApplyConfirmation(applyId, applyId)).not.toThrow();
  });
});
