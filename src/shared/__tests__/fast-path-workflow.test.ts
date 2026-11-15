/**
 * Pins the approval gate of .github/workflows/fast-path.yml: it only runs on
 * workflow_dispatch, keeps top-level permissions minimal, and nothing runs
 * before the one job that uses the fast-path environment.
 */
import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";

type Job = { environment?: string | { name?: string }; needs?: string | string[] };
const workflow = Bun.YAML.parse(
  readFileSync(new URL("../../../.github/workflows/fast-path.yml", import.meta.url), "utf8"),
) as { on: Record<string, unknown>; permissions: unknown; jobs: Record<string, Job> };
const jobs = workflow.jobs;
const envName = (job: Job) => (typeof job.environment === "object" ? job.environment?.name : job.environment);
const needsOf = (job: Job) => (job.needs === undefined ? [] : [job.needs].flat());
const gates = Object.keys(jobs).filter((id) => envName(jobs[id]) === "fast-path");

describe("fast-path workflow", () => {
  test("is triggered by workflow_dispatch only", () => {
    expect(Object.keys(workflow.on)).toEqual(["workflow_dispatch"]);
  });

  test("keeps top-level permissions to contents: read", () => {
    expect(workflow.permissions).toEqual({ contents: "read" });
  });

  test("has exactly one gate job, with no needs", () => {
    expect(gates).toHaveLength(1);
    expect(needsOf(jobs[gates[0]])).toEqual([]);
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
});
