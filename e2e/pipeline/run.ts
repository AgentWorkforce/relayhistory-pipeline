#!/usr/bin/env bun
/**
 * The full-pipeline e2e lane (docs/pipeline-e2e.md):
 *
 *   bun run e2e:pipeline                               every step, on a fresh stack on this machine
 *   bun run e2e:pipeline --target sandbox              the same assertions inside a fresh Blaxel sandbox
 *   bun run e2e:pipeline --steps upload.courier,index.hoop,summary.extractive
 *
 * `local` builds the services image, starts a fresh compose project (e2e/selfhost/compose.yaml +
 * e2e/pipeline/compose.yaml) with only the services the selected steps need, runs the steps in order and
 * removes the project and its volumes. `sandbox` runs exactly this script with `--target local` inside a
 * throwaway microVM (e2e/pipeline/sandbox.ts) and copies its report back. Either way the result is
 * report.md (a PASS/FAIL/SKIP line per step) and report.json under $RELAYFLOW_STATE_DIR/pipeline-e2e/, or
 * .relayflow/pipeline-e2e/ in the checkout. Exits 1 on any FAIL. E2E_KEEP=1 leaves a local stack up;
 * E2E_SKIP_BUILD=1 reuses a built 96bulls/services:e2e image.
 */
import { mkdirSync, rmSync, writeFileSync } from "node:fs";
import { isAbsolute, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { buildImage } from "../selfhost/stack.ts";
import { createPipelineStack } from "./stack.ts";
import { STEPS, STEP_IDS, runSteps, servicesFor, type StepResult } from "./steps.ts";

const ROOT = fileURLToPath(new URL("../..", import.meta.url));
const argv = process.argv.slice(2);
const opt = (name: string) => {
  const i = argv.indexOf(`--${name}`);
  return i >= 0 ? argv[i + 1] : undefined;
};
const target = opt("target") ?? "local";
if (target !== "local" && target !== "sandbox") {
  console.error("usage: run.ts [--target local|sandbox] [--steps id,id,...]");
  process.exit(2);
}
const wanted = opt("steps")?.split(",").map((s) => s.trim()).filter(Boolean);
const unknown = wanted?.filter((id) => !STEP_IDS.includes(id)) ?? [];
if (unknown.length) {
  console.error(`unknown step(s): ${unknown.join(", ")}; known: ${STEP_IDS.join(", ")}`);
  process.exit(2);
}
const selected = wanted?.length ? STEPS.filter((s) => wanted.includes(s.id)) : STEPS;

/** The same directory flows/lib/state-dir.mjs gives the flows. */
export function reportDir(env = process.env): string {
  const given = env.RELAYFLOW_STATE_DIR?.trim();
  const root = given ? (isAbsolute(given) ? given : resolve(ROOT, given)) : join(ROOT, ".relayflow");
  return join(root, "pipeline-e2e");
}

export function renderReport(results: StepResult[], meta: { target: string; wallMs: number; stackMs: number; selection: string; aborted?: string }): string {
  const count = (r: string) => results.filter((x) => x.result === r).length;
  const verdict = meta.aborted ? "aborted" : count("FAIL") ? `${count("FAIL")} failed` : "green";
  return [
    `# Pipeline e2e: ${verdict}`,
    "",
    `Target: ${meta.target}. Steps: ${meta.selection}. Wall clock ${Math.round(meta.wallMs / 1000)}s (stack up ${Math.round(meta.stackMs / 1000)}s).`,
    `${count("PASS")} passed, ${count("FAIL")} failed, ${count("SKIP")} skipped.`,
    ...(meta.aborted ? ["", `Aborted before the steps finished: ${meta.aborted}`] : []),
    "",
    ...results.map((r) => `- ${r.result} ${r.id} (${Math.round(r.ms / 1000)}s): ${r.title}. ${r.detail}`),
  ].join("\n") + "\n";
}

function write(results: StepResult[], meta: Parameters<typeof renderReport>[1]) {
  const dir = reportDir();
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, "report.md"), renderReport(results, meta));
  writeFileSync(join(dir, "report.json"), JSON.stringify({ ...meta, results }, null, 2));
  console.log(`\nreport: ${join(dir, "report.md")}`);
}

async function local(): Promise<boolean> {
  const started = Date.now();
  const selection = wanted?.length ? selected.map((s) => s.id).join(", ") : "all";
  const services = servicesFor(selected);
  // A report from an earlier run must never stand for this one.
  // (Only the report files: pipeline-e2e.flow.ts keeps its fleet state in the same directory.)
  for (const file of ["report.md", "report.json"]) rmSync(join(reportDir(), file), { force: true });
  if (!process.env.E2E_SKIP_BUILD) {
    console.log("building 96bulls/services:e2e ...");
    try {
      buildImage();
    } catch (error) {
      write([], { target: "local", wallMs: Date.now() - started, stackMs: 0, selection, aborted: `the services image did not build: ${(error as Error).message.split("\n")[0]}` });
      return false;
    }
  }
  const stack = createPipelineStack();
  const down = () => {
    if (process.env.E2E_KEEP === "1") return console.log(`E2E_KEEP=1: stack left up (compose project ${stack.project})`);
    stack.compose(["down", "-v", "--remove-orphans"], { quiet: true, allowFail: true });
    stack.cleanup();
  };
  for (const signal of ["SIGINT", "SIGTERM"] as const) process.on(signal, () => { down(); process.exit(130); });
  const dump = () => {
    for (const svc of ["migrate", "roles", ...services]) console.error(`--- ${svc}\n${stack.logs(svc).split("\n").slice(-60).join("\n")}`);
  };
  let results: StepResult[] = [];
  let stackMs = 0;
  try {
    console.log(`starting compose project ${stack.project}: ${services.join(", ") || "(nothing)"}`);
    if (services.length) {
      stack.compose(["up", "-d", ...services], { quiet: true });
      stack.resolvePipelineUrls(services);
      const ready = async (url: string) => {
        for (let i = 0; i < 120; i++) {
          try { if ((await fetch(url, { signal: AbortSignal.timeout(3000) })).ok) return; } catch {}
          await new Promise((r) => setTimeout(r, 2000));
        }
        throw new Error(`not ready within 240s: ${url}`);
      };
      if (services.includes("session-ingest")) await Promise.all([ready(`${stack.ingestUrl}/readyz`), ready(`${stack.urls.ingestS3}/healthz`), ready(`${stack.readUrl}/healthz`)]);
      if (services.includes("alleyoop-ingest")) await ready(`${stack.urls.alleyoop}/readyz`);
    }
    stackMs = Date.now() - started;
    results = await runSteps({ stack, state: {} }, selected, (r) => console.log(`${r.result.padEnd(4)} ${r.id}  ${r.detail}`));
    const failed = results.some((r) => r.result === "FAIL");
    if (failed) dump();
    write(results, { target: "local", wallMs: Date.now() - started, stackMs, selection });
    return !failed;
  } catch (error) {
    console.error(`pipeline e2e aborted: ${(error as Error).message}`);
    dump();
    write(results, { target: "local", wallMs: Date.now() - started, stackMs, selection, aborted: (error as Error).message });
    return false;
  } finally {
    down();
  }
}

if (import.meta.main) {
  let ok = false;
  try {
    ok = target === "local" ? await local() : await (await import("./sandbox.ts")).runInSandbox({ steps: wanted, reportDir: reportDir() });
  } catch (error) {
    console.error(`pipeline e2e (${target}): ${(error as Error).message}`);
  }
  process.exit(ok ? 0 : 1);
}
