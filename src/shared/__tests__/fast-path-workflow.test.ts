/**
 * Pins the approval gate and publish guards of .github/workflows/fast-path.yml:
 * it only runs on workflow_dispatch, keeps top-level permissions minimal,
 * nothing runs before the one job that uses the fast-path environment, only
 * Khaliq's dispatch from main reaches the publish job, and only that job holds
 * write permissions.
 */
import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";

type Step = { run?: string };
type Job = {
  if?: string;
  environment?: string | { name?: string };
  needs?: string | string[];
  permissions?: Record<string, string>;
  steps?: Step[];
};
const text = readFileSync(new URL("../../../.github/workflows/fast-path.yml", import.meta.url), "utf8");
const workflow = Bun.YAML.parse(text) as {
  on: Record<string, unknown>;
  permissions: unknown;
  jobs: Record<string, Job>;
};
const jobs = workflow.jobs;
const envName = (job: Job) => (typeof job.environment === "object" ? job.environment?.name : job.environment);
const needsOf = (job: Job) => (job.needs === undefined ? [] : [job.needs].flat());
const gates = Object.keys(jobs).filter((id) => envName(jobs[id]) === "fast-path");
const ACTOR = "github.actor == 'khaliqgant' && github.triggering_actor == 'khaliqgant'";

describe("fast-path workflow", () => {
  test("is triggered by workflow_dispatch only", () => {
    expect(Object.keys(workflow.on)).toEqual(["workflow_dispatch"]);
  });

  test("keeps top-level permissions to contents: read", () => {
    expect(workflow.permissions).toEqual({ contents: "read" });
  });

  test("has exactly one gate job, with no needs", () => {
    expect(gates).toEqual(["approve"]);
    expect(needsOf(jobs.approve)).toEqual([]);
  });

  test("makes every other job need the gate, directly or transitively", () => {
    const reaches = (id: string, seen = new Set<string>()): boolean => {
      if (id === gates[0]) return true;
      if (seen.has(id)) return false;
      seen.add(id);
      return needsOf(jobs[id]).some((dep) => reaches(dep, seen));
    };
    for (const id of Object.keys(jobs)) expect([id, reaches(id)]).toEqual([id, true]);
  });

  test("requires Khaliq as actor and triggering actor on approve and publish", () => {
    expect(jobs.approve.if).toContain(ACTOR);
    expect(jobs.publish.if).toContain(ACTOR);
  });

  test("publishes only from a main dispatch, outside image-publish", () => {
    expect(jobs.publish.if).toContain("github.ref == 'refs/heads/main'");
    expect(jobs.publish.if).toContain("needs.approve.outputs.on_main == 'true'");
    expect(envName(jobs.publish)).toBe("fast-path-publish");
    for (const [id, job] of Object.entries(jobs)) expect([id, envName(job)]).not.toEqual([id, "image-publish"]);
  });

  test("grants write permissions only to publish", () => {
    for (const [id, job] of Object.entries(jobs)) {
      const writes = Object.entries(job.permissions ?? {}).filter(([, level]) => level === "write");
      if (id === "publish") expect(writes.map(([scope]) => scope).sort()).toEqual(["id-token", "packages"]);
      else expect([id, writes]).toEqual([id, []]);
    }
  });

  test("never runs a job or step regardless of earlier results", () => {
    expect(text).not.toMatch(/\balways\(\)|\bfailure\(\)/);
  });

  test("never interpolates inputs into a run: block", () => {
    for (const [id, job] of Object.entries(jobs)) {
      for (const step of job.steps ?? []) {
        if (step.run !== undefined) expect([id, step.run]).not.toEqual([id, expect.stringMatching(/\$\{\{[^}]*inputs\./)]);
      }
    }
  });
});
