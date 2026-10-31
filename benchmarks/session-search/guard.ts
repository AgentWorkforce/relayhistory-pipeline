/**
 * The only hosts this harness may connect to. It drops and reloads schemas, so a
 * connection string for anything else is refused before a socket opens.
 */
const LOCAL = new Set(["localhost", "127.0.0.1", "[::1]", "::1"]);

export function assertLocalTarget(url: string, env: Record<string, string | undefined> = process.env): void {
  const u = new URL(url);
  if (!LOCAL.has(u.hostname)) throw new Error(`refusing non-local target ${u.hostname}`);
  if (/[?&]host=/i.test(u.search)) throw new Error("refusing host override in connection string");
  for (const v of ["PGHOST", "PGHOSTADDR", "PGSERVICE", "PGSERVICEFILE"]) {
    const value = env[v];
    if (value && !(v === "PGHOST" && LOCAL.has(value))) throw new Error(`refusing: unset ${v}`);
  }
}
