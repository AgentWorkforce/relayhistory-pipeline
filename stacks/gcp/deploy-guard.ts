import { GCP_CONFIRM_PROJECT_ENV } from "./config.ts";

export interface GcpDeploymentTarget {
  projectId: string;
  region: string;
}

/**
 * Refuse a plan or deploy before any resources are declared when the live GCP
 * project or resolved provider region is not the operator-approved target.
 * Same fail-closed shape as the AWS account and region guard.
 */
export function assertGcpDeploymentTarget(
  expected: GcpDeploymentTarget,
  actual: GcpDeploymentTarget,
): void {
  if (actual.projectId !== expected.projectId) {
    throw new Error(
      `96bulls: live GCP project is ${actual.projectId}, not approved project ${expected.projectId}. Nothing was created.`,
    );
  }
  if (actual.region !== expected.region) {
    throw new Error(
      `96bulls: resolved GCP region is ${actual.region}, not approved region ${expected.region}. Nothing was created.`,
    );
  }
}

/** Operator intent. Distinct from the live project and region check. */
export function assertGcpProjectConfirmation(projectId: string, confirmValue: string | undefined): void {
  if (confirmValue !== projectId) {
    throw new Error(
      `96bulls: set ${GCP_CONFIRM_PROJECT_ENV}=${projectId} to run this GCP stage. Nothing was created.`,
    );
  }
}
