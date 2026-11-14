/**
 * Workflow entry point (database job of .github/workflows/index-prebuild-058000.yml) for the 058000 CONCURRENTLY
 * index prebuild (src/hoopdb/index-prebuild-058000.ts; docs/packets/operator-user-erase-index-packet.md).
 *
 * Credentials. The workflow step runs `bun scripts/index-prebuild-058000.ts` with SUPABASE_MIGRATION_OWNER_URL in
 * its environment. This parent removes the URL from its own environment first, runs every check that needs no
 * credential (mode, dormant switch, sha, and for prebuild the downloaded census artifact and the byte-exact typed
 * confirmation, then the checkout pin), and only then spawns `bun scripts/index-prebuild-058000.ts --operator-stdin`
 * with fixed argv and an allowlisted environment, writing {"supabaseUrl": ...} to the child's stdin pipe from its
 * own memory. The child refuses any environment key outside the allowlist (and every credential-bearing name),
 * reads the URL only through readStdinCredentials, and connects with the guarded schema release's verify-full owner
 * client (releaseClient), proving the pinned owner identity (requireReleaseOwner) before any other query.
 *
 * Outputs (RUNNER_TEMP, outside the checkout): the census artifact (census mode) and the sanitized receipt, which a
 * prebuild rewrites after every per-index state change so a killed job still uploads how far it got. Nothing
 * credential-like is logged or written; errors print fixed codes only.
 */
import { initScrubEngine } from "../packages/pipeline/src/lib/scrub-engine/index.ts";
import { spawn } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { releaseClient, requireReleaseOwner } from "../src/hoopdb/schema-release-inspect.ts";
import { readStdinCredentials, refuseCredentialEnv } from "../src/migration/stdin-credentials.ts";
import { verifyCheckout } from "./owner-buffered-parity-operator.ts";
import {
  INDEX_PREBUILD_CENSUS_FILE_NAME,
  INDEX_PREBUILD_RECEIPT_CONTRACT,
  INDEX_PREBUILD_RECEIPT_FILE_NAME,
  indexPrebuildCode,
  indexPrebuildPreflight,
  refuseIndexPrebuild as refuse,
  runIndexPrebuild,
  type IndexPrebuildReceipt,
  type PrebuildDb,
} from "../src/hoopdb/index-prebuild-058000.ts";

const ROOT = join(import.meta.dir, "..");
/** The workflow secret. Only the parent's own environment may hold it; it reaches the child over stdin. */
export const INDEX_PREBUILD_SECRET_ENV = "SUPABASE_MIGRATION_OWNER_URL";
/** The child's whole environment: no secret, no token. */
export const INDEX_PREBUILD_CHILD_ENV = [
  "PATH", "HOME", "LANG", "TMPDIR", "RUNNER_TEMP", "GITHUB_RUN_ID", "GITHUB_SHA",
  "PREBUILD_MODE", "PREBUILD_SHA", "PREBUILD_CENSUS_RUN", "PREBUILD_CONFIRMATION",
] as const;
const CHILD_FLAG = "--operator-stdin";
const MODES = ["census", "prebuild", "readback"];

/** Spawn spec for the credential child: fixed argv, allowlisted env, the URL only in the stdin payload. */
export function indexPrebuildChildSpawn(
  env: Record<string, string | undefined>,
  supabaseUrl: string,
  script: string = join(ROOT, "scripts/index-prebuild-058000.ts"),
): { command: string; args: string[]; env: Record<string, string>; input: string } {
  const childEnv: Record<string, string> = {};
  for (const key of INDEX_PREBUILD_CHILD_ENV) if (typeof env[key] === "string") childEnv[key] = env[key]!;
  return { command: process.execPath, args: [script, CHILD_FLAG], env: childEnv, input: JSON.stringify({ supabaseUrl }) };
}

/** The child holds no credential in its environment: the URL arrives only over the stdin pipe. */
export function refuseIndexPrebuildCredentialEnv(env: Record<string, string | undefined>): void {
  refuseCredentialEnv(env);
  for (const key of Object.keys(env))
    if (!(INDEX_PREBUILD_CHILD_ENV as readonly string[]).includes(key)) refuse("index_prebuild_credential_env");
}

export function indexPrebuildPaths(env: Record<string, string | undefined>) {
  const temp = env.RUNNER_TEMP ?? "";
  if (!temp.startsWith("/")) refuse("index_prebuild_runner_temp");
  return {
    census: join(temp, "index-prebuild-census", INDEX_PREBUILD_CENSUS_FILE_NAME),
    out: join(temp, "index-prebuild-out", INDEX_PREBUILD_CENSUS_FILE_NAME),
    receipt: join(temp, "index-prebuild-out", INDEX_PREBUILD_RECEIPT_FILE_NAME),
  };
}

/** Atomic private write: a reader (the upload step after a kill) sees the previous or the next receipt, never half. */
function writePrivate(path: string, value: unknown): void {
  mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
  const temp = path + ".tmp";
  writeFileSync(temp, JSON.stringify(value, null, 2) + "\n", { mode: 0o600 });
  renameSync(temp, path);
}

function readCensus(env: Record<string, string | undefined>, path: string): unknown {
  if (env.PREBUILD_MODE !== "prebuild" || !existsSync(path)) return undefined;
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch {
    return refuse("index_prebuild_census_invalid");
  }
}

function refusedReceipt(env: Record<string, string | undefined>, code: string): IndexPrebuildReceipt {
  return {
    contract: INDEX_PREBUILD_RECEIPT_CONTRACT, status: "refused", code,
    mode: MODES.includes(env.PREBUILD_MODE ?? "") ? env.PREBUILD_MODE as IndexPrebuildReceipt["mode"] : "refused",
    runId: /^[1-9][0-9]{0,19}$/u.test(env.GITHUB_RUN_ID ?? "") ? env.GITHUB_RUN_ID! : "0",
    sourceSha: null, censusRun: null, generation: null, ledgerTail: null, ledgerPrefixDigest: null, verdict: null,
    counts: { declared: 16, present: 0, deferred: 0, required: 0, built: 0, resumed: 0, refused: 0 }, indexes: [],
    statements: 0, at: new Date().toISOString(), credentialsPersisted: false,
  };
}

function announce(receipt: IndexPrebuildReceipt): void {
  // Aggregate line only: names stay in the receipt artifact.
  console.log("index_prebuild_058000 " + JSON.stringify({
    status: receipt.status, ...(receipt.code ? { code: receipt.code } : {}), mode: receipt.mode, verdict: receipt.verdict,
    counts: receipt.counts, statements: receipt.statements,
  }));
}

/** Workflow step: credential-free checks, then pipe the one secret to the credential child. */
async function parent(): Promise<number> {
  const env = process.env;
  let receiptPath: string | null = null;
  try {
    // The secret leaves the environment first (git subprocesses below would inherit it otherwise).
    const url = env[INDEX_PREBUILD_SECRET_ENV];
    delete process.env[INDEX_PREBUILD_SECRET_ENV];
    const paths = indexPrebuildPaths(env);
    receiptPath = paths.receipt;
    const pre = indexPrebuildPreflight(env, readCensus(env, paths.census));
    verifyCheckout(pre.dispatch.sha);
    if (typeof url !== "string" || url === "") refuse("index_prebuild_credential_missing");
    const spec = indexPrebuildChildSpawn(env, url);
    return await new Promise<number>((resolve) => {
      const child = spawn(spec.command, spec.args, { cwd: ROOT, env: spec.env, stdio: ["pipe", "inherit", "inherit"] });
      child.on("error", () => resolve(1));
      child.on("close", (code) => resolve(code === null ? 1 : code));
      child.stdin.on("error", () => undefined);
      child.stdin.end(spec.input);
    });
  } catch (error) {
    const receipt = refusedReceipt(env, indexPrebuildCode(error));
    if (receiptPath) writePrivate(receiptPath, receipt);
    announce(receipt);
    return 1;
  }
}

type Client = ReturnType<typeof releaseClient>;
async function ownerSession(url: string, readOnly: boolean): Promise<Client> {
  // keepAlive: a build session is silent for up to the statement budget; an idle-dropped connection would turn a
  // healthy build into an unknown outcome.
  const client = releaseClient(url, { keepAlive: !readOnly });
  try {
    await client.connect();
    await requireReleaseOwner(client);
    if (readOnly) {
      await client.query("SET default_transaction_read_only = on");
      const ro = (await client.query<{ ro: string }>("SELECT current_setting('default_transaction_read_only') AS ro")).rows[0];
      if (ro?.ro !== "on") refuse("index_prebuild_not_read_only");
    }
    return client;
  } catch (error) {
    await client.end().catch(() => undefined);
    throw error;
  }
}

/** Credential child: the URL arrives over stdin only. */
async function operatorChild(): Promise<number> {
  const env = process.env;
  let receiptPath: string | null = null;
  try {
    const paths = indexPrebuildPaths(env);
    receiptPath = paths.receipt;
    refuseIndexPrebuildCredentialEnv(env);
    const census = readCensus(env, paths.census);
    const pre = indexPrebuildPreflight(env, census);
    verifyCheckout(pre.dispatch.sha);
    const input = await readStdinCredentials(["supabaseUrl"]);
    const url = typeof input.supabaseUrl === "string" ? input.supabaseUrl : refuse("index_prebuild_credential_missing");
    const prebuild = pre.dispatch.mode === "prebuild";
    const client = await ownerSession(url, !prebuild);
    try {
      const outcome = await runIndexPrebuild(env, {
        db: client as unknown as PrebuildDb,
        fresh: async () => {
          const c = await ownerSession(url, true);
          return { db: c as unknown as PrebuildDb, close: () => c.end() };
        },
      }, { runId: env.GITHUB_RUN_ID ?? "", census, persist: (r) => writePrivate(paths.receipt, r) });
      writePrivate(paths.receipt, outcome.receipt);
      if (outcome.census) writePrivate(paths.out, outcome.census);
      announce(outcome.receipt);
      return outcome.exitCode;
    } finally {
      await client.end().catch(() => undefined);
    }
  } catch (error) {
    const receipt = refusedReceipt(env, indexPrebuildCode(error));
    if (receiptPath) writePrivate(receiptPath, receipt);
    announce(receipt);
    return 1;
  }
}

function dieQuietly(): never {
  console.log("index_prebuild_058000 " + JSON.stringify({ status: "refused", code: "index_prebuild_failed" }));
  process.exit(1);
}

if (import.meta.main) {
  process.on("uncaughtException", dieQuietly);
  process.on("unhandledRejection", dieQuietly);
  // Scrub engine (docs/scrub-engine.md, "Default flip"): the pinned wasm-bindgen engine, read from the repo checkout.
  await initScrubEngine().catch(dieQuietly);
  process.exit(process.argv.includes(CHILD_FLAG) ? await operatorChild() : await parent());
}
