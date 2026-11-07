/**
 * Neon session census loader (046000 sessions.session_neon_census). The LEAD runs this; it is a
 * live HOLD until Khaliq's explicit OK for the production run.
 *
 * stdin: ONE JSON line {"neonUrl": "...", "supabaseUrl": "..."} (RAM only). Never pass
 * credentials on argv, in env files or logs; this tool never prints them or any census row.
 *   neonUrl      a Neon login; the tool only ever opens a READ ONLY REPEATABLE READ transaction
 *   supabaseUrl  the Supabase OWNER login (the load/seal/digest functions are owner-only);
 *                required for --execute and --verify only
 *
 *   bun scripts/neon-census-load.ts                 dry run (default): Neon census counts + digest; writes nothing
 *   bun scripts/neon-census-load.ts --pin           pin THIS Neon as the authoritative production source: stdin must
 *                                                   also carry "expectedSourceIdentity" (the out-of-band verified
 *                                                   production identity the dry run prints); any field differing refuses
 *   bun scripts/neon-census-load.ts --execute       load the census in batches (resumable, idempotent) and seal it
 *                                                   (the seal refuses any snapshot not from the pinned source)
 *   bun scripts/neon-census-load.ts --verify        recompute from Neon; compare with the seal and the sealed rows
 *                                                   (exit 2 on any mismatch, 3 when nothing is sealed)
 * Options: --batch N (default 5000, max 10000), --fetch N (cursor page, default 5000),
 *          --statement-timeout-ms N (Neon, default 900000).
 *
 * The census generation is its digest: rerunning --execute against the same frozen Neon resumes
 * the same unsealed generation; a sealed generation is immutable, and a changed Neon is a new
 * generation whose seal replaces the old one.
 */
import { readFileSync } from "node:fs";
import pg from "pg";
import { loadAndSeal, pinSource, readNeonCensus, verifyCensus, type Queryable } from "../src/migration/neon-census.ts";

/** A connection the CLI opens (production: pg.Client; tests inject PGlite-backed fakes). */
export interface CensusClient extends Queryable {
  connect(): Promise<unknown>;
  end(): Promise<unknown>;
}
export interface CensusCliDeps {
  client(url: string, applicationName: string): CensusClient;
  out(line: string): void;
  err(line: string): void;
}

class Exit extends Error {
  constructor(readonly code: number) {
    super(`exit ${code}`);
  }
}

export type CensusMode = "dry-run" | "execute" | "verify" | "pin";
export interface CensusArgs {
  mode: CensusMode;
  batch: number;
  fetch: number;
  statementTimeoutMs: number;
}

/**
 * Pure argv preflight, run BEFORE stdin (the credentials) is read: unknown or credential-like
 * arguments (never echoed), conflicting modes and bad numbers refuse here, so an invalid
 * invocation never consumes or holds the credential document.
 */
export function parseCensusArgs(args: readonly string[]): { ok: true; args: CensusArgs } | { ok: false; error: string } {
  const numeric = new Set(["--batch", "--fetch", "--statement-timeout-ms"]);
  const known = new Set(["--execute", "--verify", "--pin", ...numeric]);
  const values: Record<string, number> = { "--batch": 5000, "--fetch": 5000, "--statement-timeout-ms": 900_000 };
  for (let i = 0; i < args.length; i += 1) {
    const a = args[i]!;
    // Never echo the argument: it may be a credential pasted on argv.
    if (!known.has(a)) return { ok: false, error: `unknown argument at position ${i + 1} (credentials go on stdin only)` };
    if (numeric.has(a)) {
      const n = Number(args[i + 1]);
      if (!Number.isInteger(n) || n <= 0) return { ok: false, error: `${a} must be a positive integer` };
      values[a] = n;
      i += 1;
    }
  }
  const modes = (["--execute", "--verify", "--pin"] as const).filter((m) => args.includes(m));
  if (modes.length > 1) return { ok: false, error: "one of --execute | --verify | --pin (or none for a dry run)" };
  const mode: CensusMode = modes[0] === "--execute" ? "execute" : modes[0] === "--verify" ? "verify" : modes[0] === "--pin" ? "pin" : "dry-run";
  return { ok: true, args: { mode, batch: values["--batch"]!, fetch: values["--fetch"]!, statementTimeoutMs: values["--statement-timeout-ms"]! } };
}

/**
 * The whole CLI. `readStdin` is called ONLY after the arguments are accepted; a failed read is a
 * fixed, bounded refusal (exit 66). Returns the exit code.
 */
export async function runCensusCli(args: string[], readStdin: () => string | Promise<string>, deps: CensusCliDeps): Promise<number> {
  const fail = (line: string, code = 64): never => {
    deps.err(line);
    throw new Exit(code);
  };
  const parsed = parseCensusArgs(args);
  if (!parsed.ok) {
    deps.err(parsed.error);
    return 64;
  }
  const mode = parsed.args.mode;
  try {
    const val = (k: "--batch" | "--fetch" | "--statement-timeout-ms") =>
      k === "--batch" ? parsed.args.batch : k === "--fetch" ? parsed.args.fetch : parsed.args.statementTimeoutMs;
    const log = (m: string, f: Record<string, unknown> = {}) => deps.out(JSON.stringify({ at: new Date().toISOString(), mode, m, ...f }));

    let stdinText = "";
    try {
      stdinText = await readStdin();
    } catch {
      fail("stdin could not be read", 66);
    }
    let input: { neonUrl?: string; supabaseUrl?: string; expectedSourceIdentity?: Record<string, unknown> } = {};
    try {
      input = JSON.parse(stdinText.split("\n")[0] || "{}");
    } catch {
      fail("stdin must be one JSON line");
    }
    stdinText = "";
    const neonUrl = input.neonUrl ?? "";
    const supabaseUrl = input.supabaseUrl ?? "";
    const expectedSourceIdentity = input.expectedSourceIdentity;
    input = {};
    if (!/^postgres(ql)?:\/\//.test(neonUrl)) fail("neonUrl required on stdin");
    if (mode !== "dry-run" && !/^postgres(ql)?:\/\//.test(supabaseUrl)) fail("supabaseUrl (owner) required on stdin for --execute/--verify/--pin");
    if (mode === "pin" && (typeof expectedSourceIdentity !== "object" || expectedSourceIdentity === null))
      fail("--pin needs expectedSourceIdentity (the verified production identity) on stdin");

    const neon = deps.client(neonUrl, "neon-census-readonly");
    await neon.connect();
    let census;
    try {
      census = await readNeonCensus(neon, {
        fetch: val("--fetch"), statementTimeoutMs: val("--statement-timeout-ms"), endpoint: new URL(neonUrl).hostname,
      });
    } finally {
      await neon.end().catch(() => {});
    }
    log("neon census", { rows: census.count, sha256: census.sha256, snapshot: census.snapshot.snapshot, read_at: census.snapshot.read_at, lsn: census.snapshot.lsn,
      source_identity: census.snapshot.source_identity });
    if (mode === "dry-run") return 0;
    const supa = deps.client(supabaseUrl, "neon-census-load");
    await supa.connect();
    try {
      if (mode === "pin") {
        const out = await pinSource(supa, census, expectedSourceIdentity!);
        log(out.pinned ? "census source pinned" : "census source REFUSED: the live Neon is not the expected production source", out);
        return out.pinned ? 0 : 2;
      }
      if (mode === "execute") {
        const out = await loadAndSeal(supa, census, { batch: val("--batch"), log });
        log("census sealed", out);
        return out.sealed ? 0 : 1;
      }
      const v = await verifyCensus(supa, census);
      log("census verify", v);
      return v.ok ? 0 : v.reason === "census_unsealed" ? 3 : 2;
    } finally {
      await supa.end().catch(() => {});
    }
  } catch (error) {
    if (error instanceof Exit) return error.code;
    // The message only: driver errors can echo connection parameters in their properties.
    deps.err(JSON.stringify({ mode, error: error instanceof Error ? error.message.replace(/postgres(ql)?:\/\/\S+/g, "<url>") : "failed" }));
    return 1;
  }
}

if (import.meta.main) {
  // stdin is read only inside runCensusCli, after the argv preflight accepted the invocation.
  const code = await runCensusCli(process.argv.slice(2), () => readFileSync(0, "utf8"), {
    client: (url, applicationName) => new pg.Client({ connectionString: url, ssl: { rejectUnauthorized: true }, application_name: applicationName }),
    out: (line) => console.log(line),
    err: (line) => console.error(line),
  });
  process.exit(code);
}
