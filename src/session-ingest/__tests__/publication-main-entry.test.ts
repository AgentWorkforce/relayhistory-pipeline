/**
 * publication-main.ts as the CLI entry (docs/scrub-engine.md "Default flip"): importing the module
 * initialises no scrub engine and declares no TypeScript capability; the entry initialises the engine
 * before publicationMain runs a publish cycle (order asserted with fakes), and a disabled run, `--gc`
 * or invalid arguments never need the engine. No AWS, DB or store.
 */
import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { initScrubEngine as seamInitScrubEngine } from "../../../packages/pipeline/src/lib/scrub-engine/index.ts";
import { initScrubEngine, publicationEntry, publicationMain } from "../publication-main.ts";

const ROOT = fileURLToPath(new URL("../../../", import.meta.url));

function fakes(initError?: Error) {
  const calls: string[] = [];
  const deps = {
    init: async () => { calls.push("init"); if (initError) throw initError; },
    main: (async (args: string[] = []) => { calls.push(`main:${args.join(" ")}`); return { enabled: true, cycles: 1 }; }) as typeof publicationMain,
  };
  return { calls, deps };
}

test("importing publication-main initialises no engine and declares no capability (clean process)", () => {
  // A child process without the test preload: this process's preload grants the "test" capability.
  const proc = Bun.spawnSync([process.execPath, "-e", `
    await import("./src/session-ingest/publication-main.ts");
    const cap = await import("./packages/pipeline/src/lib/scrub-engine/ts-capability.ts");
    const seam = await import("./packages/pipeline/src/lib/scrub-engine/index.ts");
    let sealer = "built";
    try { seam.routedRawChunkSealerDeps(); } catch (e) { sealer = e.code; }
    console.log(JSON.stringify({ caps: cap.scrubTsCapabilities(), ready: seam.scrubEngineReady(), log: seam.scrubTextForLog("x"), sealer }));
  `], { cwd: ROOT, env: { PATH: process.env.PATH ?? "", HOME: process.env.HOME ?? "" }, stdout: "pipe", stderr: "pipe" });
  expect(proc.stderr.toString()).toBe("");
  expect(proc.exitCode).toBe(0);
  expect(JSON.parse(proc.stdout.toString().trim())).toStrictEqual({ caps: [], ready: false, log: "[scrub-unavailable]", sealer: "scrub_engine_unavailable" });
});

test("publish enabled: the engine is initialised before publicationMain runs", async () => {
  for (const args of [[], ["--continuous"]]) {
    const { calls, deps } = fakes();
    expect(await publicationEntry(args, { INGEST_PUBLISH_ENABLED: "true" }, deps)).toStrictEqual({ enabled: true, cycles: 1 });
    expect(calls).toStrictEqual(["init", `main:${args.join(" ")}`]);
  }
});

test("an engine that fails to initialise stops the entry before any cycle", async () => {
  const { calls, deps } = fakes(Object.assign(new Error("pin"), { code: "bindgen_pin_not_exact" }));
  await expect(publicationEntry([], { INGEST_PUBLISH_ENABLED: "true" }, deps)).rejects.toMatchObject({ code: "bindgen_pin_not_exact" });
  expect(calls).toStrictEqual(["init"]);
});

test("disabled publish and --gc run without the engine", async () => {
  const cases: [string[], NodeJS.ProcessEnv][] = [
    [[], {}],
    [[], { INGEST_PUBLISH_ENABLED: "false" }],
    [["--gc"], { INGEST_PUBLISH_ENABLED: "true", INGEST_PUBLICATION_GC_ENABLED: "true" }],
    [["--gc", "--continuous"], { INGEST_PUBLISH_ENABLED: "true" }],
  ];
  for (const [args, env] of cases) {
    const { calls, deps } = fakes();
    await publicationEntry(args, env, deps);
    expect(calls).toStrictEqual([`main:${args.join(" ")}`]);
  }
});

test("invalid arguments and an invalid flag fail as publicationMain does, before the engine", async () => {
  let inits = 0;
  const init = async () => { inits++; };
  await expect(publicationEntry(["--bogus"], { INGEST_PUBLISH_ENABLED: "true" }, { init, main: publicationMain })).rejects.toThrow("publication_arguments");
  await expect(publicationEntry([], { INGEST_PUBLISH_ENABLED: "yes" }, { init, main: publicationMain })).rejects.toThrow("enabled_invalid");
  expect(await publicationEntry([], {}, { init, main: publicationMain })).toStrictEqual({ enabled: false });
  expect(inits).toBe(0);
});

test("the module runs publicationEntry (not publicationMain) under import.meta.main, and its default init is initScrubEngine", () => {
  const src = readFileSync(`${ROOT}src/session-ingest/publication-main.ts`, "utf8");
  expect(src).toMatch(/^if\(import\.meta\.main\)publicationEntry\(\)\.catch\(/m);
  expect(src).not.toMatch(/import\.meta\.main\)\s*publicationMain\(/);
  expect(src).toMatch(/\{init:\(\)=>initScrubEngine\(\),main:publicationMain\}/);
});

test("publication-main re-exports the seam's own initScrubEngine (the compiled-mode proof initialises the bundle's copy through it)", () => {
  expect(typeof initScrubEngine).toBe("function");
  expect(initScrubEngine).toBe(seamInitScrubEngine);
});
