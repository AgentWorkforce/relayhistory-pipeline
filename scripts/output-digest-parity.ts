/**
 * Operator-only parity check for lane F4: the same read against relayhistory (RHC, Neon)
 * and the 96Bulls read API, with ONE bearer token read from stdin (never argv, env or
 * disk), compared field by field. Prints content-free verdicts only: field names and
 * equal/differs, never values. Read-only GETs; nothing is written anywhere.
 *
 *   printf %s "$TOKEN" | bun scripts/output-digest-parity.ts \
 *     --rhc https://<rhc-origin> --sp https://<read-api-origin> --workspace <id> \
 *     --since 2026-11-05T00:00:00Z --until 2026-11-05T23:59:59.999Z \
 *     [--date 2026-11-05 --tz UTC]
 *
 * Output: `GET /v1/sessions/output` must be exactly equal (`output.*`, since, until).
 * Digest (with --date): `stats`, `projects`, `epics`, `sessions`, `date`, `tz`, `window`
 * must be equal; `narrative` is reported separately (model text may differ by design),
 * and `stats.outcomes.reverted` is excluded while the field is temporarily omitted.
 * Exit 0 = all required fields equal; 1 = a difference; 2 = usage or a non-200.
 */

export interface ParityLine {
  route: "output" | "digest";
  field: string;
  equal: boolean;
  required: boolean;
}

const stable = (v: unknown): string =>
  JSON.stringify(v, (_k, x) => (x && typeof x === "object" && !Array.isArray(x) ? Object.fromEntries(Object.entries(x).sort(([a], [b]) => a.localeCompare(b))) : x));

/** Compare two Output bodies: every `output` field plus the echoed window. */
export function compareOutput(rhc: Record<string, any>, sp: Record<string, any>): ParityLine[] {
  const fields = ["agentMessages", "commits", "linesAdded", "linesRemoved", "commitsWithoutLineCounts"];
  return [
    ...["since", "until"].map((f) => ({ route: "output" as const, field: f, equal: rhc[f] === sp[f], required: true })),
    ...fields.map((f) => ({ route: "output" as const, field: `output.${f}`, equal: rhc.output?.[f] === sp.output?.[f], required: true })),
  ];
}

/** Compare two digest bodies. `reverted` is excluded while omitted ("drop and rebuild"). */
export function compareDigest(rhc: Record<string, any>, sp: Record<string, any>): ParityLine[] {
  const strip = (stats: any) => {
    if (!stats || typeof stats !== "object") return stats;
    const { outcomes, ...rest } = stats;
    if (!outcomes || typeof outcomes !== "object") return rest;
    const { reverted: _r, ...o } = outcomes;
    return { ...rest, outcomes: o };
  };
  const lines: ParityLine[] = ["date", "tz", "window", "scope", "projects", "epics", "sessions"].map((f) => ({
    route: "digest", field: f, equal: stable(rhc[f]) === stable(sp[f]), required: true,
  }));
  const rs = strip(rhc.stats) ?? {};
  const ss = strip(sp.stats) ?? {};
  for (const k of [...new Set([...Object.keys(rs), ...Object.keys(ss)])].sort())
    lines.push({ route: "digest", field: `stats.${k}`, equal: stable(rs[k]) === stable(ss[k]), required: true });
  lines.push({ route: "digest", field: "narrative.status", equal: rhc.narrative?.status === sp.narrative?.status, required: false });
  lines.push({ route: "digest", field: "narrative", equal: stable(rhc.narrative) === stable(sp.narrative), required: false });
  return lines;
}

function arg(argv: string[], name: string): string | undefined {
  const i = argv.indexOf(`--${name}`);
  return i >= 0 ? argv[i + 1] : undefined;
}

async function readToken(): Promise<string> {
  const chunks: Uint8Array[] = [];
  for await (const c of process.stdin) chunks.push(c as Uint8Array);
  return Buffer.concat(chunks).toString("utf8").trim();
}

async function getJson(origin: string, path: string, token: string): Promise<Record<string, any>> {
  const r = await fetch(new URL(path, origin), { headers: { authorization: `Bearer ${token}` } });
  if (r.status !== 200) {
    console.error(`non-200 from ${new URL(origin).host}${new URL(path, origin).pathname}: ${r.status}`);
    process.exit(2);
  }
  return (await r.json()) as Record<string, any>;
}

export async function main(argv = process.argv.slice(2)): Promise<number> {
  const rhc = arg(argv, "rhc"), sp = arg(argv, "sp"), workspace = arg(argv, "workspace");
  const since = arg(argv, "since"), until = arg(argv, "until"), date = arg(argv, "date"), tz = arg(argv, "tz") ?? "UTC";
  if (!rhc || !sp || !workspace || !since || !until) {
    console.error("usage: --rhc <origin> --sp <origin> --workspace <id> --since <iso> --until <iso> [--date YYYY-MM-DD --tz <iana>] (token on stdin)");
    return 2;
  }
  const token = await readToken();
  if (!token) { console.error("no token on stdin"); return 2; }
  const q = (p: Record<string, string>) => new URLSearchParams({ workspace, ...p }).toString();
  const outPath = `/v1/sessions/output?${q({ since, until })}`;
  const lines = compareOutput(await getJson(rhc, outPath, token), await getJson(sp, outPath, token));
  if (date) {
    const digestPath = `/v1/digest/daily?${q({ date, tz })}`;
    lines.push(...compareDigest(await getJson(rhc, digestPath, token), await getJson(sp, digestPath, token)));
  }
  for (const l of lines) console.log(`${l.route} ${l.field} ${l.equal ? "equal" : "DIFFERS"}${l.required ? "" : " (informational)"}`);
  return lines.some((l) => l.required && !l.equal) ? 1 : 0;
}

if (import.meta.main) process.exit(await main());
