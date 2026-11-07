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
import { loadAndSeal, readNeonCensus, verifyCensus } from "../src/migration/neon-census.ts";

const args = process.argv.slice(2);
const flag = (k: string) => args.includes(k);
const val = (k: string, d: number) => {
  const i = args.indexOf(k);
  const n = i >= 0 ? Number(args[i + 1]) : d;
  if (!Number.isInteger(n) || n <= 0) {
    console.error(`${k} must be a positive integer`);
    process.exit(64);
  }
  return n;
};
const known = new Set(["--execute", "--verify", "--pin", "--batch", "--fetch", "--statement-timeout-ms"]);
for (let i = 0; i < args.length; i += 1) {
  if (!known.has(args[i]!)) {
    // Never echo the argument: it may be a credential pasted on argv.
    console.error(`unknown argument at position ${i + 1} (credentials go on stdin only)`);
    process.exit(64);
  }
  if (["--batch", "--fetch", "--statement-timeout-ms"].includes(args[i]!)) i += 1;
}
if ([flag("--execute"), flag("--verify"), flag("--pin")].filter(Boolean).length > 1) {
  console.error("one of --execute | --verify | --pin (or none for a dry run)");
  process.exit(64);
}
const mode = flag("--execute") ? "execute" : flag("--verify") ? "verify" : flag("--pin") ? "pin" : "dry-run";
const log = (m: string, f: Record<string, unknown> = {}) => console.log(JSON.stringify({ at: new Date().toISOString(), mode, m, ...f }));

let input: { neonUrl?: string; supabaseUrl?: string; expectedSourceIdentity?: Record<string, unknown> } = {};
try {
  input = JSON.parse(readFileSync(0, "utf8").split("\n")[0] || "{}");
} catch {
  console.error("stdin must be one JSON line");
  process.exit(64);
}
const neonUrl = input.neonUrl ?? "";
const supabaseUrl = input.supabaseUrl ?? "";
const expectedSourceIdentity = input.expectedSourceIdentity;
input = {};
if (!/^postgres(ql)?:\/\//.test(neonUrl)) {
  console.error("neonUrl required on stdin");
  process.exit(64);
}
if (mode !== "dry-run" && !/^postgres(ql)?:\/\//.test(supabaseUrl)) {
  console.error("supabaseUrl (owner) required on stdin for --execute/--verify/--pin");
  process.exit(64);
}
if (mode === "pin" && (typeof expectedSourceIdentity !== "object" || expectedSourceIdentity === null)) {
  console.error("--pin needs expectedSourceIdentity (the verified production identity) on stdin");
  process.exit(64);
}

const client = (url: string, name: string) =>
  new pg.Client({ connectionString: url, ssl: { rejectUnauthorized: true }, application_name: name });

async function main(): Promise<number> {
  const neon = client(neonUrl, "neon-census-readonly");
  await neon.connect();
  let census;
  try {
    census = await readNeonCensus(neon, {
      fetch: val("--fetch", 5000), statementTimeoutMs: val("--statement-timeout-ms", 900_000), endpoint: new URL(neonUrl).hostname,
    });
  } finally {
    await neon.end().catch(() => {});
  }
  log("neon census", { rows: census.count, sha256: census.sha256, snapshot: census.snapshot.snapshot, read_at: census.snapshot.read_at, lsn: census.snapshot.lsn,
    source_identity: census.snapshot.source_identity });
  if (mode === "dry-run") return 0;
  const supa = client(supabaseUrl, "neon-census-load");
  await supa.connect();
  try {
    if (mode === "pin") {
      const out = await pinSource(supa, census, expectedSourceIdentity!);
      log(out.pinned ? "census source pinned" : "census source REFUSED: the live Neon is not the expected production source", out);
      return out.pinned ? 0 : 2;
    }
    if (mode === "execute") {
      const out = await loadAndSeal(supa, census, { batch: val("--batch", 5000), log });
      log("census sealed", out);
      return out.sealed ? 0 : 1;
    }
    const v = await verifyCensus(supa, census);
    log("census verify", v);
    return v.ok ? 0 : v.reason === "census_unsealed" ? 3 : 2;
  } finally {
    await supa.end().catch(() => {});
  }
}

main().then(
  (code) => process.exit(code),
  (error) => {
    // The message only: driver errors can echo connection parameters in their properties.
    console.error(JSON.stringify({ mode, error: error instanceof Error ? error.message.replace(/postgres(ql)?:\/\/\S+/g, "<url>") : "failed" }));
    process.exit(1);
  },
);
