import { describe, expect, it } from "vitest";
import { runtimeConformance, type Observed } from "../src/runtime/conformance.js";
import {
  createWorkerRuntime,
  settle,
  type PipelineWork,
  type WorkItem,
  type WorkOutcome,
  type WorkSource,
} from "../src/runtime/index.js";

/** In-memory stand-in for the HoopDb pipeline_jobs queue. */
function memorySource(keys: readonly string[]) {
  const queue: WorkItem[] = keys.map((key, i) => ({ type: "manifest-landed", id: `job-${i}`, key, attempt: 1 }));
  const settled = new Map<string, WorkOutcome>();
  const source: WorkSource = {
    async claim(max) {
      return queue.splice(0, max);
    },
    async settle(item, outcome) {
      settled.set(item.id, outcome);
    },
  };
  return { source, settled, queue };
}

const observe = (o: WorkOutcome): Observed => (o.status === "done" ? "acked" : o.status === "retry" ? "redeliver" : "dead");

runtimeConformance("workers (in-memory queue)", async () => ({
  adapter: createWorkerRuntime({ source: memorySource([]).source }),
  async deliverManifests(work, keys) {
    const m = memorySource(keys);
    const binding = createWorkerRuntime({ source: m.source, batchSize: 10, now: () => 0 }).bind({ ...work, schedules: [] });
    await binding.tick();
    const out = new Map<string, Observed>();
    keys.forEach((key, i) => out.set(key, observe(m.settled.get(`job-${i}`)!)));
    return out;
  },
  async fireSchedule(work, job) {
    if (!work.schedules.some((s) => s.job === job)) throw new Error("undeclared schedule");
    const only: PipelineWork = { ...work, schedules: work.schedules.filter((s) => s.job === job) };
    await createWorkerRuntime({ source: memorySource([]).source, now: () => 0 }).bind(only).tick();
  },
}));

describe("worker runtime specifics", () => {
  it("fires each schedule once per window and again in the next window", async () => {
    let t = 0;
    const fired: string[] = [];
    const work: PipelineWork = {
      schedules: [{ job: "reconcile", everySeconds: 300 }],
      async handle(items) {
        for (const i of items) if (i.type === "schedule") fired.push(`${i.job}@${i.firedAtMs}`);
        return new Map(items.map((i) => [i.id, { status: "done" } as WorkOutcome]));
      },
    };
    const b = createWorkerRuntime({ source: memorySource([]).source, now: () => t }).bind(work);
    await b.tick();
    t = 299_999;
    await b.tick();
    t = 300_000;
    await b.tick();
    expect(fired).toEqual(["reconcile@0", "reconcile@300000"]);
  });

  it("rejects malformed schedules at bind time", () => {
    const rt = createWorkerRuntime({ source: memorySource([]).source });
    const handle = async () => new Map();
    expect(() => rt.bind({ handle, schedules: [{ job: "Bad Name", everySeconds: 1 }] })).toThrow();
    expect(() => rt.bind({ handle, schedules: [{ job: "a", everySeconds: 0 }] })).toThrow();
    expect(() => rt.bind({ handle, schedules: [{ job: "a", everySeconds: 1 }, { job: "a", everySeconds: 2 }] })).toThrow();
  });

  it("run() stops when the signal aborts", async () => {
    const controller = new AbortController();
    const b = createWorkerRuntime({ source: memorySource([]).source, idleMs: 1 }).bind({ handle: async () => new Map(), schedules: [] });
    const running = b.run(controller.signal);
    controller.abort();
    await expect(running).resolves.toBeUndefined();
  });

  it("settle() turns a bogus outcome into a retry", async () => {
    const item: WorkItem = { type: "manifest-landed", id: "x", key: "k", attempt: 1 };
    const out = await settle(
      { schedules: [], handle: async () => new Map([["x", { status: "maybe" } as unknown as WorkOutcome]]) },
      [item],
      { runtime: "test" },
    );
    expect(out.get("x")).toMatchObject({ status: "retry", code: "no_outcome" });
  });
});
