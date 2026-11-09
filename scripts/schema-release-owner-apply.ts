/** Separate existing-owner apply entry; 200's PLAN ONLY helper is untouched. */
import { execFileSync } from "node:child_process";
import { readFile, writeFile, mkdir } from "node:fs/promises";
import { join } from "node:path";
import pg from "pg";
import { parseCliLogin } from "./schema-release-owner-operator-plan.ts";
import { generation } from "../src/hoopdb/migrate.ts";
import { SUPABASE_PROD_CA_2021 } from "../src/session-pipeline/supabase-ca.ts";
import { SCHEMA_RELEASE as P } from "../src/hoopdb/schema-release-contract.ts";
import { releaseErrorCode } from "../src/hoopdb/schema-release-inspect.ts";
import {
  OWNER_APPLY as A,
  digest,
  ownerApplyRefuse as no,
  wrapperSource,
  validateReadonlyPlan,
  validateOwnerApply,
  withOwnerSession,
  inspectOwner,
  ownerCapture,
  applyOwnerRelease,
  auditCanonicalOwnerSql,
  validateOwnerIntegration,
  type OwnerApplyIntegration,
} from "../src/hoopdb/owner-operator-apply.ts";
export type { OwnerApplyIntegration, OwnerPreparedContext } from "../src/hoopdb/owner-operator-apply.ts";
const CLI = "/home/khaliqgant/.local/bin/supabase";
const INPUT = ".96bulls/owner-apply/readonly-plan.json",
  CAPTURE = "docs/plans/supabase-owner-apply.capture.json",
  APPROVAL = ".96bulls/owner-apply/approval.json";
/** Supabase CLI refreshes this version-only cache during its approved login flow. */
const CLI_CACHE = "supabase/.temp/cli-latest";
export function ownerApplySourceFiles(files: string[], cache?: string): void {
  if (files.some(p => p !== CAPTURE && p !== CLI_CACHE)) no("owner_dirty_checkout");
  if (files.includes(CLI_CACHE) && (typeof cache !== "string" || cache.length > 64 || !/^v[0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9.-]+)?\n?$/u.test(cache))) no("owner_cli_cache_invalid");
}
export function ownerApplyArgs(argv: string[]) {
  const mode = argv[0];
  if (
    (mode !== "capture" && mode !== "apply") ||
    argv[1] !== "--source-sha" ||
    argv[3] !== "--generation" ||
    !/^([0-9a-f]{40})$/u.test(argv[2] ?? "") ||
    !/^([0-9a-f]{64})$/u.test(argv[4] ?? "") ||
    (mode === "capture" && argv.length !== 5) ||
    (mode === "apply" &&
      (argv.length !== 9 ||
        argv[5] !== "--capture-sha" ||
        argv[7] !== "--approval-sha" ||
        !/^[0-9a-f]{64}$/u.test(argv[6] ?? "") ||
        !/^[0-9a-f]{64}$/u.test(argv[8] ?? "")))
  )
    no("owner_apply_args");
  return {
    mode,
    pin: { sha: argv[2]!, generation: argv[4]! },
    captureSha: argv[6],
    approvalSha: argv[8],
  };
}
async function text(path: string) {
  const bytes = await readFile(path);
  if (bytes.byteLength > 2 * 1024 * 1024) no("owner_artifact_bound");
  try {
    return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch {
    return no("owner_artifact_utf8");
  }
}
export async function ownerApplyMain(argv = process.argv.slice(2), integration?: OwnerApplyIntegration) {
  const input = ownerApplyArgs(argv);
  validateOwnerIntegration(integration);
  if (integration !== undefined && input.mode !== "apply") no("owner_integration_apply_only");
  const childEnv = Object.fromEntries(
    [
      "PATH",
      "HOME",
      "XDG_CONFIG_HOME",
      "XDG_DATA_HOME",
      "DBUS_SESSION_BUS_ADDRESS",
    ].flatMap((k) => (process.env[k] ? [[k, process.env[k]!]] : [])),
  );
  const run = (exe: string, args: string[]) =>
    execFileSync(exe, args, {
      env: childEnv,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
      timeout: 30000,
      maxBuffer: 65536,
    });
  const recheckSource = async () => {
    if (
      run("git", ["rev-parse", "HEAD"]).trim() !== input.pin.sha ||
      (await generation()) !== input.pin.generation
    )
      no("owner_checkout_generation");
    const files = [
      ...run("git", ["diff", "HEAD", "--name-only", "-z"]).split("\0"),
      ...run("git", ["ls-files", "--others", "--exclude-standard", "-z"]).split(
        "\0",
      ),
    ].filter(Boolean);
    ownerApplySourceFiles(files, files.includes(CLI_CACHE) ? await text(CLI_CACHE) : undefined);
  };
  await recheckSource();
  const planText = await text(INPUT),
    plan = validateReadonlyPlan(planText, input.pin);
  await auditCanonicalOwnerSql();
  const wrapper = await wrapperSource();
  let captureText = "",
    approvalText = "";
  if (input.mode === "apply") {
    captureText = await text(CAPTURE);
    approvalText = await text(APPROVAL);
    if (
      digest(captureText) !== input.captureSha ||
      digest(approvalText) !== input.approvalSha
    )
      no("owner_artifact_digest");
    validateOwnerApply(planText, captureText, approvalText, input.pin, wrapper);
  }
  // All local source/plan/manual-exception gates precede even temporary login.
  if (run(CLI, ["--version"]).trim() !== "2.122.0") no("owner_cli_version");
  let raw = run(CLI, [
    "db",
    "dump",
    "--linked",
    "--project-ref",
    A.project,
    "--dry-run",
  ]);
  const login = parseCliLogin(raw);
  raw = "";
  const client = new pg.Client({
    host: P.host,
    port: 5432,
    database: A.database,
    user: A.login + "." + A.project,
    password: login.password,
    ssl: {
      ca: SUPABASE_PROD_CA_2021,
      rejectUnauthorized: true,
      servername: P.host,
    },
    connectionTimeoutMillis: 10000,
    query_timeout: 300000,
    options:
      "-c statement_timeout=300000 -c lock_timeout=5000 -c idle_in_transaction_session_timeout=60000",
    application_name: "96bulls-owner-schema-" + input.mode,
  });
  login.password = "";
  client.on("error", () => {});
  return withOwnerSession(client, async (pid) => {
    if (input.mode === "capture") {
      const inspection = await inspectOwner(client, pid);
      const capture = ownerCapture(
        planText,
        plan,
        input.pin,
        inspection,
        wrapper,
      );
      await recheckSource();
      const body = JSON.stringify(capture, null, 2) + "\n";
      await mkdir(join("docs", "plans"), { recursive: true });
      await writeFile(CAPTURE, body, { mode: 0o600 });
      return {
        status: "owner_apply_capture_readonly_not_approval",
        sourceSha: input.pin.sha,
        generation: input.pin.generation,
        ledgerSha256: A.ledger,
        captureSha256: digest(body),
        captureGeneration: capture.captureGeneration,
        wrapperSha256: wrapper.sha256,
      };
    }
    return applyOwnerRelease(client, pid, {
      pin: input.pin,
      planText,
      captureText,
      approvalText,
      wrapper,
      recheckSource,
      integration,
    });
  });
}
if (import.meta.main)
  ownerApplyMain()
    .then((report) => console.log(JSON.stringify(report)))
    .catch((error) => {
      console.log(
        JSON.stringify({ status: "refused", code: releaseErrorCode(error) }),
      );
      process.exitCode = 1;
    });
