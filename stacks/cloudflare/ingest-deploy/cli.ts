/**
 * CLI for .github/workflows/deploy-session-ingest.yml. Read-only against Cloudflare (GET only);
 * the deploy itself is the workflow's wrangler step. See plan.ts and
 * runbooks/sessions-prod-session-ingest.md.
 *
 *   plan    --out-dir <dir>                                   resolve, render, guard, print the sanitized diff
 *   recheck --out-dir <dir> --expect-version <id> --expect-sha <sha256>   right before deploy: live unchanged, same render
 *   verify  --out-dir <dir> --previous <id>                   after deploy: the new active version carries exactly the render
 *
 * Env: CLOUDFLARE_API_TOKEN, CLOUDFLARE_ACCOUNT_ID (environment secrets of `session-ingest`), the
 * IN_* dispatch inputs (plan.ts INPUT_ENV), GITHUB_OUTPUT. Nothing it prints is a live value.
 */
import { createHash } from "node:crypto";
import { appendFileSync, chmodSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { SESSION_INGEST_WORKER } from "../session-ingest.ts";
import {
  IngestDeployError,
  activeVersionId,
  assertDiffAllowed,
  bindingsAfterDeploy,
  parseCommittedProfile,
  parseDispatchInputs,
  parseLiveSettings,
  parseLiveVersion,
  planDeploy,
  renderedJson,
  sanitizedDiff,
  valuesToMask,
  type LiveBinding,
  type WranglerConfig,
} from "./plan.ts";

const REPO_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");
export const PROFILE_PATH = join(REPO_ROOT, "deploy/session-ingest/production-profile.json");
const need: (ok: unknown, why: string) => asserts ok = (ok, why) => {
  if (!ok) throw new IngestDeployError(why);
};

type Get = (path: string) => Promise<unknown>;

function cloudflareGet(env: NodeJS.ProcessEnv): Get {
  const account = env.CLOUDFLARE_ACCOUNT_ID ?? "";
  need(/^[0-9a-f]{32}$/.test(account), "CLOUDFLARE_ACCOUNT_ID (environment secret) must be the 32-hex account id");
  need(env.CLOUDFLARE_API_TOKEN, "CLOUDFLARE_API_TOKEN (environment secret) is required for the live read");
  const root = `https://api.cloudflare.com/client/v4/accounts/${account}/workers/scripts/${SESSION_INGEST_WORKER}`;
  return async (path) => {
    const response = await fetch(root + path, {
      method: "GET",
      headers: { Authorization: `Bearer ${env.CLOUDFLARE_API_TOKEN}` },
      redirect: "error",
      signal: AbortSignal.timeout(10_000),
    });
    need(response.ok, `cannot read the live Worker (${path.split("/")[1] ?? "script"}: HTTP ${response.status})`);
    const raw = await response.text();
    need(raw.length <= 2 * 1024 * 1024, "live Worker response too large");
    const value = JSON.parse(raw) as { success?: unknown; result?: unknown };
    need(value.success === true && value.result, "Cloudflare refused the live Worker read");
    return value.result;
  };
}

async function readLive(get: Get, versionId?: string) {
  const id = versionId ?? activeVersionId(await get("/deployments"));
  const version = parseLiveVersion(await get(`/versions/${id}`), id);
  const settings = parseLiveSettings(await get("/settings"));
  return { version, settings };
}

export async function plan(env: NodeJS.ProcessEnv, get: Get, outDir: string, profileRaw = readFileSync(PROFILE_PATH, "utf8")) {
  const profile = parseCommittedProfile(profileRaw);
  const inputs = parseDispatchInputs(env);
  const { version, settings } = await readLive(get);
  const paths = {
    main: relative(outDir, join(REPO_ROOT, "src/session-ingest/worker.ts")),
    tsconfig: relative(outDir, join(REPO_ROOT, "tsconfig.json")),
  };
  const result = planDeploy(version, settings, profile, inputs, paths);
  const json = renderedJson(result.config);
  return { ...result, version, inputs, json, sha256: createHash("sha256").update(json).digest("hex"), masks: valuesToMask(version) };
}

function arg(argv: string[], name: string): string {
  const i = argv.indexOf(name);
  const v = i >= 0 ? argv[i + 1] : undefined;
  need(v && !v.startsWith("--"), `missing ${name}`);
  return v;
}

function output(env: NodeJS.ProcessEnv, lines: Record<string, string>) {
  need(env.GITHUB_OUTPUT, "GITHUB_OUTPUT is required");
  for (const [k, v] of Object.entries(lines)) need(/^[a-z][a-z0-9_]*$/.test(k) && /^[A-Za-z0-9._-]*$/.test(v), "unsafe workflow output");
  appendFileSync(env.GITHUB_OUTPUT, Object.entries(lines).map(([k, v]) => `${k}=${v}\n`).join(""));
}

function mask(env: NodeJS.ProcessEnv, values: readonly string[]) {
  // Masks first, before anything a live value could appear in (the wrangler steps print bindings).
  if (env.GITHUB_ACTIONS === "true") for (const v of values) console.log(`::add-mask::${v}`);
}

/** The commands; `get` is injectable for tests, otherwise the GET-only Cloudflare reader. */
export async function main(argv: string[], env: NodeJS.ProcessEnv, injected?: Get) {
  const command = argv[0];
  const outDir = resolve(arg(argv, "--out-dir"));
  const configPath = join(outDir, "wrangler.json");
  const get = injected ?? cloudflareGet(env);
  if (command === "plan") {
    const p = await plan(env, get, outDir);
    mask(env, p.masks);
    const diff = sanitizedDiff(p.decisions);
    console.log(`Live version: ${p.liveVersionId} (the only version at 100%)`);
    console.log("Knob resolution (explicit input > committed profile > live):");
    for (const d of p.decisions) console.log(`  ${d.knob}: live ${d.liveValue}${d.liveRaw === undefined ? " (unset)" : ""} -> ${d.renderedValue} [${d.source}]`);
    console.log("Every other variable, binding and setting is copied from live; the guard checked it is unchanged.");
    if (diff.length === 0) console.log("Sanitized diff vs live: EMPTY. The render equals the live configuration.");
    else {
      console.log("Sanitized diff vs live:");
      for (const line of diff) console.log(`  ${line}`);
    }
    assertDiffAllowed(p.decisions);
    mkdirSync(outDir, { recursive: true, mode: 0o700 });
    writeFileSync(configPath, p.json, { mode: 0o600 });
    chmodSync(configPath, 0o600);
    output(env, { live_version: p.liveVersionId, render_sha256: p.sha256, diff_empty: diff.length === 0 ? "true" : "false", dry_run: p.inputs.dryRun ? "true" : "false" });
    console.log(`Render sha256: ${p.sha256}; no provider writes.`);
    return;
  }
  if (command === "recheck") {
    const expectVersion = arg(argv, "--expect-version");
    const expectSha = arg(argv, "--expect-sha");
    const p = await plan(env, get, outDir);
    mask(env, p.masks);
    need(p.liveVersionId === expectVersion, "the live Worker changed since the plan; refusing to deploy (re-dispatch)");
    assertDiffAllowed(p.decisions);
    need(p.sha256 === expectSha, "the render changed since the plan; refusing to deploy (re-dispatch)");
    need(readFileSync(configPath, "utf8") === p.json, "the rendered config on disk differs from the plan; refusing to deploy");
    console.log(`Live version ${expectVersion} unchanged and render identical; deploying.`);
    return;
  }
  if (command === "verify") {
    const previous = arg(argv, "--previous");
    const config = JSON.parse(readFileSync(configPath, "utf8")) as WranglerConfig;
    const before = parseLiveVersion(await get(`/versions/${previous}`), previous);
    const { version, settings } = await readLive(get);
    mask(env, valuesToMask(version));
    need(version.id !== previous, "the active version is still the previous one; the deploy did not take effect");
    need(
      version.compatibilityDate === config.compatibility_date &&
        JSON.stringify(version.compatibilityFlags) === JSON.stringify(config.compatibility_flags) &&
        JSON.stringify(version.limits) === JSON.stringify(config.limits),
      "the new version's compatibility settings or limits differ from the render; roll back (runbook)",
    );
    need(
      settings.placementMode === "smart" &&
        JSON.stringify(settings.observability) === JSON.stringify(config.observability) &&
        settings.logpush === config.logpush &&
        JSON.stringify(settings.tailConsumers) === JSON.stringify(config.tail_consumers),
      "the Worker settings after the deploy differ from the render; roll back (runbook)",
    );
    const key = (b: LiveBinding) => JSON.stringify(Object.fromEntries(Object.entries(b).sort(([a], [z]) => a.localeCompare(z))));
    const want = bindingsAfterDeploy(config, before.bindings).map(key).sort();
    const got = version.bindings.map(key).sort();
    need(JSON.stringify(want) === JSON.stringify(got), "the new version's bindings differ from the render; roll back (runbook)");
    console.log(`New active version ${version.id} at 100% carries exactly the rendered bindings. Previous version (rollback handle): ${previous}.`);
    return;
  }
  throw new IngestDeployError("unsupported command (plan, recheck, verify)");
}

if (process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1])) {
  try {
    await main(process.argv.slice(2), process.env);
  } catch (error) {
    const reason = error instanceof IngestDeployError ? error.message : "the live Worker could not be read or parsed";
    console.error(`::error::session-ingest deploy refused: ${reason}. No deploy.`);
    process.exitCode = 1;
  }
}
