/**
 * Shared guard for opt-in real-PostgreSQL tests: the same `owned_loopback_fixture_required` predicate
 * the *.postgres.test.ts files under src/session-ingest/__tests__ inline (e.g. claim-clock-lock,
 * and acquisition-owner on #332). Accepts only postgres:/postgresql:, a loopback host, user "postgres",
 * no password, database "/postgres", no query string and no hash, so URL options cannot override the
 * host, user or password and nothing falls back to PG* environment identities. Throws before any connection.
 */
export function ownedLoopbackFixture(raw: string): URL {
  let u: URL;
  try { u = new URL(raw); } catch { throw Error("owned_loopback_fixture_required"); }
  if (!["postgresql:", "postgres:"].includes(u.protocol) || !["127.0.0.1", "localhost", "[::1]"].includes(u.hostname) || u.username !== "postgres" ||
      u.password || u.pathname !== "/postgres" || u.search || u.hash) throw Error("owned_loopback_fixture_required");
  return u;
}
