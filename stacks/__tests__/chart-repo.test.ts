import { describe, expect, test } from "bun:test";
import { CHART_REPO } from "../aws/eks.ts";
import { isPublishedHelmRepo, PUBLISHED_HELM_REPO } from "../gcp/config.ts";

describe("default chart repository", () => {
  test("AWS and GCP default to the same published index", () => {
    expect(CHART_REPO).toBe("https://agentworkforce.github.io/infra");
    expect(PUBLISHED_HELM_REPO).toBe(CHART_REPO);
  });

  test("the GCP published-repo check follows the move and refuses the dead path", () => {
    expect(isPublishedHelmRepo(undefined)).toBe(true);
    expect(isPublishedHelmRepo(PUBLISHED_HELM_REPO)).toBe(true);
    expect(isPublishedHelmRepo(`${PUBLISHED_HELM_REPO}/`)).toBe(true);
    expect(isPublishedHelmRepo("https://AgentWorkforce.github.io/helm-charts")).toBe(false);
    expect(isPublishedHelmRepo("https://example.com/infra")).toBe(false);
  });
});
