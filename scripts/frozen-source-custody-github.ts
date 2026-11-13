/**
 * Read-only GitHub gate of the frozen-source custody workflow (.github/workflows/frozen-source-custody.yml).
 * Without --protected it is the prerequisite gate (no environment, no secret); with --protected it also proves the
 * environment approval and this run's deployment. The operator re-proves all of it (verifySupervisor) before a write.
 * Output: fixed codes and check names only, never a value.
 */
import { githubGet } from "./schema-release-github.ts";
import { verifyCustodyGithub } from "../src/migration/frozen-source-copy-supervisor.ts";

export async function frozenSourceCustodyGate(
  env: Record<string, string | undefined>,
  get: (path: string) => Promise<any>,
  protectedJob: boolean,
): Promise<{ ok: true } | { ok: false; code: string; check: string }> {
  try {
    await verifyCustodyGithub(env, get, { protectedJob });
    return { ok: true };
  } catch (e) {
    const code = String((e as { code?: unknown })?.code ?? "");
    const check = String((e as { detail?: { check?: unknown } })?.detail?.check ?? "");
    return code.startsWith("frozen_copy_supervisor_") && /^[a-z_]+$/.test(check)
      ? { ok: false, code, check }
      : { ok: false, code: "frozen_copy_supervisor_missing", check: "github_api" };
  }
}

if (import.meta.main) {
  const protectedJob = process.argv.includes("--protected");
  let get: (path: string) => Promise<any>;
  try {
    get = githubGet(process.env.GITHUB_TOKEN);
  } catch {
    get = async () => { throw new Error("token"); };
  }
  frozenSourceCustodyGate(process.env, get, protectedJob).then((r) => {
    if (r.ok) console.log("frozen_source_custody_gate_ok");
    else {
      console.error(`frozen_source_custody_gate_refused ${r.code} ${r.check}`);
      process.exitCode = 1;
    }
  });
}
