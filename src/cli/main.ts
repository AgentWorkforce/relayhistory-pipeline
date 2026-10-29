/**
 * `96bulls`: one entry point for the sessions pipeline.
 *
 *   96bulls up <stage> [--reseed]     stand the whole pipe up (local: Docker via Alchemy)
 *   96bulls down <stage> [--purge]    tear it down; --purge also forgets local secrets and seed
 *   96bulls status <stage>            what is running and whether it is ready
 *   96bulls smoke <stage>             follow one session end to end and check telemetry for PII
 *   96bulls seed <stage> [--fresh]    (re)build the Concourse fixture history.db
 *   96bulls token <stage>             mint a courier token for manual calls
 *   96bulls plan <stage>              alchemy plan (dry run)
 *
 * Guardrail: stages named aws* refuse to deploy until the AWS account work is
 * cleared by Khaliq (docs/aws-stage.md).
 */
import { spawnSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { generateLocalKeys, mintToken } from "../alleyoop/auth.ts";
import { seedConcourse } from "../concourse/seed.ts";
import { HOOPDB_NAME, HOOPDB_USER, NAMES, PORTS, TENANT } from "../../stacks/local/topology.ts";

const ROOT = fileURLToPath(new URL("../..", import.meta.url));
const args = process.argv.slice(2);
const [command, stageArg] = args;
const flags = new Set(args.slice(2).filter((a) => a.startsWith("--")));

function die(message: string, code = 2): never {
  console.error(`96bulls: ${message}`);
  process.exit(code);
}

function sh(cmd: string, cmdArgs: string[], opts: { cwd?: string; env?: NodeJS.ProcessEnv; quiet?: boolean; allowFail?: boolean } = {}) {
  const r = spawnSync(cmd, cmdArgs, {
    cwd: opts.cwd ?? ROOT,
    env: { ...process.env, ...opts.env },
    stdio: opts.quiet ? ["ignore", "pipe", "pipe"] : "inherit",
    encoding: "utf8",
  });
  if (r.status !== 0 && !opts.allowFail) die(`${cmd} ${cmdArgs.join(" ")} failed (exit ${r.status})`, r.status ?? 1);
  return r;
}

const docker = (...a: string[]) => sh("docker", a, { quiet: true, allowFail: true });

interface Inputs {
  hoopdbPassword: string;
  lockerAccessKeyId: string;
  lockerSecretAccessKey: string;
  jwksJson: string;
  jwtKid: string;
  jwtPrivateKeyPem: string;
  courierToken: string;
  concourseDir: string;
  telemetryDir: string;
  deploymentId: string;
  machineId: string;
}

function stageDir(stage: string) {
  return join(ROOT, ".96bulls", stage);
}

function requireLocalStage(stage: string | undefined): string {
  if (!stage) die("stage is required, e.g. `96bulls up local`");
  if (stage.startsWith("aws")) die("AWS stages are not cleared yet; nothing was created (docs/aws-stage.md)");
  if (stage !== "local" && !stage.startsWith("local-")) die(`unknown stage ${stage}; use local or local-<name>`);
  return stage;
}

function suffixed(stage: string, base: string) {
  return stage === "local" ? base : `${base}-${stage.slice("local-".length)}`;
}

/** Secrets and paths for the stage. Created once, reused until --purge. Tokens are re-minted on every up. */
function prepareInputs(stage: string): Inputs {
  const dir = stageDir(stage);
  mkdirSync(dir, { recursive: true, mode: 0o700 });
  const path = join(dir, "inputs.json");
  let inputs: Partial<Inputs> = {};
  if (existsSync(path)) inputs = JSON.parse(readFileSync(path, "utf8")) as Inputs;
  if (!inputs.jwtPrivateKeyPem) {
    const keys = generateLocalKeys();
    inputs.jwtPrivateKeyPem = keys.privateKeyPem;
    inputs.jwtKid = keys.kid;
    inputs.jwksJson = JSON.stringify(keys.jwks);
  }
  inputs.hoopdbPassword ||= randomBytes(18).toString("base64url");
  inputs.lockerAccessKeyId ||= `locker${randomBytes(6).toString("hex")}`;
  inputs.lockerSecretAccessKey ||= randomBytes(24).toString("base64url");
  inputs.concourseDir ||= join(dir, "concourse");
  inputs.telemetryDir ||= join(dir, "telemetry");
  inputs.deploymentId ||= `${stage}-${randomBytes(4).toString("hex")}`;
  inputs.machineId ||= `m-${randomBytes(8).toString("hex")}`;
  inputs.courierToken = mintToken({ kid: inputs.jwtKid!, privateKeyPem: inputs.jwtPrivateKeyPem }, {
    issuer: TENANT.issuer,
    audience: "relayhistory",
    orgId: TENANT.orgId,
    workspaceId: TENANT.workspaceId,
    userId: TENANT.userId,
    ttlSeconds: 7 * 24 * 3600,
  });
  mkdirSync(inputs.concourseDir, { recursive: true });
  mkdirSync(inputs.telemetryDir, { recursive: true });
  writeFileSync(path, JSON.stringify(inputs, null, 2), { mode: 0o600 });
  return inputs as Inputs;
}

export function loadInputs(stage: string): Inputs {
  const path = join(stageDir(stage), "inputs.json");
  if (!existsSync(path)) die(`${path} is missing; run \`96bulls up ${stage}\` first`);
  return JSON.parse(readFileSync(path, "utf8")) as Inputs;
}

export function hostHoopDbUrl(stage: string, inputs = loadInputs(stage)) {
  return `postgres://${HOOPDB_USER}:${inputs.hoopdbPassword}@127.0.0.1:${PORTS.hoopdb}/${HOOPDB_NAME}`;
}

function preflight() {
  const checks: Array<[string, () => boolean]> = [
    ["docker daemon", () => docker("info", "--format", "{{.ServerVersion}}").status === 0],
    ["alchemy cli", () => existsSync(join(ROOT, "node_modules/alchemy/package.json"))],
    ["vendored relayhistory-cloud", () => existsSync(join(ROOT, "vendor/relayhistory-cloud/package.json"))],
    ["vendored deps installed", () => existsSync(join(ROOT, "vendor/relayhistory-cloud/node_modules/hono/package.json"))],
  ];
  for (const [label, ok] of checks) if (!ok()) die(`preflight failed: ${label}`);
  for (const [label, port] of Object.entries(PORTS)) {
    const r = spawnSync("sh", ["-c", `ss -ltn 2>/dev/null | awk '{print $4}' | grep -q ':${port}$'`]);
    if (r.status === 0) {
      const mine = docker("ps", "--filter", `label=96bulls.stack`, "--format", "{{.Ports}}").stdout?.includes(`:${port}->`);
      if (!mine) die(`preflight failed: host port ${port} (${label}) is in use by something that is not this stack`);
    }
  }
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function waitFor(label: string, check: () => boolean | Promise<boolean>, timeoutMs: number) {
  const started = Date.now();
  process.stdout.write(`  waiting for ${label} `);
  for (;;) {
    if (await check()) {
      console.log(`ok (${Math.round((Date.now() - started) / 1000)}s)`);
      return;
    }
    if (Date.now() - started > timeoutMs) {
      console.log("timeout");
      die(`${label} did not become ready in ${timeoutMs / 1000}s`);
    }
    process.stdout.write(".");
    await sleep(2_000);
  }
}

function containerState(name: string): { status?: string; health?: string; exit?: number } {
  const r = docker("inspect", "--format", "{{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}} {{.State.ExitCode}}", name);
  if (r.status !== 0) return {};
  const [status, health, exit] = (r.stdout ?? "").trim().split(" ");
  return { status, health, exit: Number(exit) };
}

async function readiness(stage: string) {
  const n = (b: string) => suffixed(stage, b);
  await waitFor("hoopdb", () => containerState(n(NAMES.hoopdb)).health === "healthy", 120_000);
  await waitFor("locker", () => containerState(n(NAMES.locker)).health === "healthy", 120_000);
  await waitFor("locker bucket", () => {
    const s = containerState(n(NAMES.lockerInit));
    return s.status === "exited" && s.exit === 0;
  }, 120_000);
  await waitFor("hoopdb migrations", () => {
    const s = containerState(n(NAMES.hoopdbMigrate));
    if (s.status === "exited" && s.exit !== 0) die(`migrations failed; see: docker logs ${n(NAMES.hoopdbMigrate)}`);
    return s.status === "exited" && s.exit === 0;
  }, 600_000);
  await waitFor("alleyoop ingest /readyz", async () => {
    try {
      return (await fetch(`http://127.0.0.1:${PORTS.ingest}/readyz`)).status === 200;
    } catch {
      return false;
    }
  }, 180_000);
  for (const c of [NAMES.indexer, NAMES.referee, NAMES.bobCostas, NAMES.courier, NAMES.otel])
    await waitFor(c, () => containerState(n(c)).status === "running", 60_000);
}

async function seed(stage: string, fresh: boolean) {
  const inputs = loadInputs(stage);
  const r = await seedConcourse({ dir: inputs.concourseDir, fresh });
  writeFileSync(join(stageDir(stage), "seed.json"), JSON.stringify({ seededAt: new Date().toISOString(), ...r }, null, 2));
  console.log(`  concourse: ${r.sessions.length} sessions in ${r.dbPath}`);
}

async function up(stage: string) {
  preflight();
  console.log(`96bulls up ${stage}`);
  const inputs = prepareInputs(stage);
  const seedPath = join(stageDir(stage), "seed.json");
  if (flags.has("--reseed") || !existsSync(seedPath) || !existsSync(join(inputs.concourseDir, "history.db"))) await seed(stage, true);
  sh("bunx", ["alchemy", "deploy", "--stage", stage, "--yes", "--no-input"], { env: { ALCHEMY_PLAIN: "1" } });
  await readiness(stage);
  console.log("\n96bulls is up:");
  console.log(`  hoopdb   postgres://${HOOPDB_USER}:***@127.0.0.1:${PORTS.hoopdb}/${HOOPDB_NAME}`);
  console.log(`  locker   http://127.0.0.1:${PORTS.locker}  bucket=locker`);
  console.log(`  ingest   http://127.0.0.1:${PORTS.ingest}  (/healthz, /readyz, /v1/pipeline/*)`);
  console.log(`  otel     http://127.0.0.1:${PORTS.otelHttp}`);
  console.log(`  secrets  ${join(stageDir(stage), "inputs.json")} (mode 600, gitignored)`);
  console.log(`\nnext: 96bulls smoke ${stage}`);
}

async function down(stage: string) {
  console.log(`96bulls down ${stage}`);
  // Stop everything first so destroy only has to remove stopped containers. A
  // forced removal of a running container under a busy daemon has been seen to
  // sit in "removal in progress" for 20+ minutes on this machine.
  const running = (docker("ps", "-q", "--filter", `label=96bulls.stack=${stage}`).stdout ?? "").split("\n").filter(Boolean);
  if (running.length) docker("stop", "-t", "15", ...running);
  if (!existsSync(join(stageDir(stage), "inputs.json"))) {
    console.log("  nothing recorded for this stage; removing any leftover containers by label");
  } else {
    sh("bunx", ["alchemy", "destroy", "--stage", stage, "--yes", "--no-input"], { env: { ALCHEMY_PLAIN: "1" }, allowFail: true });
  }
  await waitFor("container removal", () => {
    const left = (docker("ps", "-aq", "--filter", `label=96bulls.stack=${stage}`).stdout ?? "").split("\n").filter(Boolean);
    if (!left.length) return true;
    // Only force what destroy left and is not already being removed.
    for (const id of left) {
      const st = (docker("inspect", "--format", "{{.State.Status}} {{.State.Removing}}", id).stdout ?? "").trim();
      if (!st.includes("true") && !st.startsWith("dead")) docker("rm", "-f", id);
    }
    return false;
  }, 600_000);
  // The courier's ledger is kept: ids stay stable, and the courier re-uploads on its
  // own when it finds its manifests gone from a fresh Locker.
  await waitFor("container removal", () => {
    const left = (docker("ps", "-aq", "--filter", `label=96bulls.stack=${stage}`).stdout ?? "").split("\n").filter(Boolean);
    if (!left.length) return true;
    // Only force what destroy left and is not already being removed.
    for (const id of left) {
      const st = (docker("inspect", "--format", "{{.State.Status}} {{.State.Removing}}", id).stdout ?? "").trim();
      if (!st.includes("true") && !st.startsWith("dead")) docker("rm", "-f", id);
    }
    return false;
  }, 600_000);
  // The courier's upload ledger belongs to the stack that is gone: a fresh stack re-ingests Concourse.
  try {
    const inputs = loadInputs(stage);
    rmSync(join(inputs.concourseDir, ".alleyoop-courier.json"), { force: true });
  } catch {
    /* no inputs: nothing to reset */
  }
  if (flags.has("--purge")) {
    for (const v of [NAMES.hoopdbData, NAMES.lockerData, NAMES.telemetry]) docker("volume", "rm", "-f", suffixed(stage, v));
    rmSync(stageDir(stage), { recursive: true, force: true });
    rmSync(join(ROOT, ".alchemy", "state", "96bulls", stage), { recursive: true, force: true });
    console.log("  purged secrets, seed and volumes");
  }
  console.log("  down");
}

function status(stage: string) {
  const r = docker("ps", "-a", "--filter", `label=96bulls.stack=${stage}`, "--format", "table {{.Names}}\t{{.Status}}\t{{.Ports}}");
  console.log(r.stdout || "(no containers)");
}

async function main() {
  switch (command) {
    case "up":
      return up(requireLocalStage(stageArg));
    case "down":
      return await down(requireLocalStage(stageArg));
    case "status":
      return status(requireLocalStage(stageArg));
    case "plan": {
      if (!stageArg) die("stage is required");
      if (stageArg.startsWith("aws-")) {
        // Code-only until cleared: plan needs credentials and an aws-inputs.json written by hand.
        return void sh("bunx", ["alchemy", "plan", "--config", "stacks/aws/alchemy.aws.run.ts", "--stage", stageArg, "--no-input"], { env: { ALCHEMY_PLAIN: "1" }, allowFail: true });
      }
      return void sh("bunx", ["alchemy", "plan", "--stage", requireLocalStage(stageArg), "--no-input"], { env: { ALCHEMY_PLAIN: "1" } });
    }
    case "seed":
      requireLocalStage(stageArg);
      prepareInputs(stageArg!);
      return seed(stageArg!, flags.has("--fresh"));
    case "token": {
      const inputs = loadInputs(requireLocalStage(stageArg));
      console.log(mintToken({ kid: inputs.jwtKid, privateKeyPem: inputs.jwtPrivateKeyPem }, {
        issuer: TENANT.issuer, audience: "relayhistory", orgId: TENANT.orgId, workspaceId: TENANT.workspaceId, userId: TENANT.userId, ttlSeconds: 3600,
      }));
      return;
    }
    case "sandbox": {
      // Plan only until the AWS path is cleared; prints the steps it would take.
      const { planSandbox } = await import("./sandbox.ts");
      const sub = args[1] as "up" | "down" | "extend" | "status";
      const agent = args[2] ?? "";
      const hours = Number(args[args.indexOf("--hours") + 1]) || undefined;
      const plan = planSandbox(sub, agent, { hours });
      console.log(`96bulls sandbox ${plan.command} ${plan.agent} (stage ${plan.stage}) -- PLAN ONLY, nothing executed:`);
      for (const step of plan.steps) console.log(`  - ${step}`);
      return;
    }
    case "smoke": {
      const stage = requireLocalStage(stageArg);
      const { runSmoke } = await import("../smoke/e2e.ts");
      const ok = await runSmoke(stage);
      process.exit(ok ? 0 : 1);
    }
    default:
      die("usage: 96bulls up|down|status|plan|seed|token|smoke <stage> [--reseed|--purge|--fresh] | sandbox up|down|extend|status <agent>");
  }
}

await main();
