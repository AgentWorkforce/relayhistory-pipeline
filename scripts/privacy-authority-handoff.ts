/**
 * C3 operator CLI (lead only; every subcommand but `census` is a production write to Supabase, never to Neon).
 *
 *   <creds> | bun run scripts/privacy-authority-handoff.ts census   --epoch <hex64>
 *   <creds> | bun run scripts/privacy-authority-handoff.ts fence    --epoch <hex64>
 *   <creds> | bun run scripts/privacy-authority-handoff.ts sync-owned --epoch <hex64>   (needs neonUrl too; run while fenced)
 *   <creds> | bun run scripts/privacy-authority-handoff.ts activate --epoch <hex64> --expect-neon <fingerprint> [--settle-seconds 120]
 *   <creds> | bun run scripts/privacy-authority-handoff.ts release  --epoch <hex64>
 *
 * stdin (RAM only, never a file or env): {"neonUrl": "<read-only-capable Neon URL>", "supabaseUrl": "<Supabase owner URL>"}
 * Both must be DEDICATED direct or session-mode connections (session settings, a session advisory lock and one
 * cursor must stay on one backend). Known transaction poolers (Supabase :6543, pgbouncer=true, Neon -pooler hosts)
 * are refused before any connection; sync-owned also verifies its backend pid at runtime.
 * (`release` and `fence` need only supabaseUrl). Output: one JSON line, aggregate only.
 * Exit: 0 done / parity · 3 census differs · 1 refused or failed · 64 usage.
 */
import pg from "pg";
import { activate, census, fence, release, syncOwned, HandoffRefusal } from "../src/migration/privacy-authority-handoff.ts";
import { pinnedPgClient, readStdinCredentials, refuseCredentialEnv, sessionAffinityUrl, CredentialRefusal } from "../src/migration/stdin-credentials.ts";

const HEX64 = /^[0-9a-f]{64}$/;
const out = (fields: Record<string, unknown>) => process.stdout.write(JSON.stringify({ at: new Date().toISOString(), tool: "privacy-authority-handoff", ...fields }) + "\n");
function arg(name: string): string | undefined {
  const i = process.argv.indexOf(`--${name}`);
  return i > 0 ? process.argv[i + 1] : undefined;
}

async function connect(url: string, name: string, app: string, readOnly: boolean) {
  // Password pinned from the stdin URL (never ~/.pgpass); user and host must be explicit (sessionAffinityUrl).
  const c = pinnedPgClient(pg, url, name, { application_name: app, statement_timeout: 20_000 });
  await c.connect();
  await c.query(`SET lock_timeout = 5000; SET idle_in_transaction_session_timeout = 60000${readOnly ? "; SET default_transaction_read_only = on" : ""}`);
  if (readOnly) {
    const ro = (await c.query("SELECT current_setting('default_transaction_read_only') AS ro")).rows[0]?.ro;
    if (ro !== "on") throw new HandoffRefusal("neon_session_not_read_only");
  }
  return c;
}

async function main(): Promise<number> {
  const cmd = process.argv[2];
  const epoch = arg("epoch");
  if (!cmd || !["census", "fence", "sync-owned", "activate", "release"].includes(cmd) || !epoch || !HEX64.test(epoch)) {
    out({ refused: "usage" });
    return 64;
  }
  refuseCredentialEnv();
  const needsNeon = cmd === "census" || cmd === "activate" || cmd === "sync-owned";
  const creds = await readStdinCredentials(needsNeon ? ["neonUrl", "supabaseUrl"] : ["supabaseUrl"], ["neonUrl", "supabaseUrl"]);
  const clients: pg.Client[] = [];
  try {
    // Both URLs are checked before EITHER connection is opened.
    const supabaseUrl = sessionAffinityUrl(creds.supabaseUrl, "supabaseUrl", "supabase");
    const neonUrl = needsNeon ? sessionAffinityUrl(creds.neonUrl, "neonUrl", "neon") : undefined;
    const supa = await connect(supabaseUrl, "supabaseUrl", "c3-privacy-authority-handoff", cmd === "census");
    clients.push(supa);
    const neon = neonUrl ? await connect(neonUrl, "neonUrl", "c3-privacy-authority-handoff-ro", true) : undefined;
    if (neon) clients.push(neon);
    delete creds.neonUrl;
    delete creds.supabaseUrl;
    if (cmd === "census") {
      const c = await census(neon!, supa, epoch);
      out({ m: "census", ...c });
      return c.equal && c.ownedEqual ? 0 : 3;
    }
    if (cmd === "fence") {
      out({ m: "fenced", ...(await fence(supa, epoch)) });
      return 0;
    }
    if (cmd === "sync-owned") {
      out({ m: "owned_synced", ...(await syncOwned(neon!, supa)) });
      return 0;
    }
    if (cmd === "release") {
      out({ m: "released", ...(await release(supa, epoch)) });
      return 0;
    }
    const expect = arg("expect-neon");
    const settle = Number(arg("settle-seconds") ?? "120");
    if (!expect || !HEX64.test(expect) || !Number.isInteger(settle) || settle < 30) {
      out({ refused: "usage_activate" });
      return 64;
    }
    const r = await activate(neon!, supa, { epoch, expectNeonFingerprint: expect, settleMs: settle * 1000 });
    out({ m: r.neonStableAfter ? "activated" : "STOP_neon_changed_after_activation", ...r });
    return r.neonStableAfter ? 0 : 1;
  } catch (error) {
    const code =
      error instanceof HandoffRefusal || error instanceof CredentialRefusal
        ? error.code
        : typeof (error as { code?: unknown })?.code === "string" && /^[0-9A-Z]{5}$/.test((error as { code: string }).code)
          ? `sqlstate_${(error as { code: string }).code}`
          : "error";
    // Our own refusals are static "frozen source: ..." texts with no identifiers; anything else is code-only.
    const message = String((error as Error)?.message ?? "");
    out({ m: "STOPPED", refused: code, ...(/^frozen source: [a-z ;:-]+$/.test(message) ? { reason: message } : {}) });
    return 1;
  } finally {
    for (const c of clients) await c.end().catch(() => undefined);
  }
}

if (import.meta.main) process.exit(await main());
