/**
 * Cross-tenant tests for sessions.search_session_directory through real PostgREST,
 * real Postgres and RS256 tokens. Nothing here sets request.jwt.claims by hand.
 * Keys are generated per run and only live in a temp directory. No hosted database.
 *
 * Needs PG_BIN_DIR (initdb, pg_ctl, postgres) and POSTGREST_BIN, which
 * scripts/fetch-postgrest-test-bins.sh provides. To use a throwaway server instead of
 * a local one, such as CloudNativePG on a local cluster, set SD_TEST_PG_ADMIN_URL to a
 * superuser URL and SD_TEST_PG_DISPOSABLE=yes; PG_BIN_DIR is then not needed. The suite
 * refuses a database that already has a `sessions` schema, and it leaves its roles
 * behind, so each run needs a fresh cluster. Two PostgREST
 * instances run against one database:
 * - `selfHosted` loads stacks/aws/session-directory-postgrest unchanged.
 * - `broad` is shaped like a project with weaker settings: anon, service_role and a
 *   second trusted key, no jwt-aud. It proves the RPC's own pins hold when PostgREST
 *   lets a token through.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { SQL, spawn, type Subprocess } from "bun";
import { createHmac, createSign, generateKeyPairSync, type KeyObject } from "node:crypto";
import { mkdtempSync, readFileSync, readdirSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";

const root = join(import.meta.dir, "../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const PROFILE = "stacks/aws/session-directory-postgrest";
const PG_BIN_DIR = process.env.PG_BIN_DIR;
const POSTGREST_BIN = process.env.POSTGREST_BIN;
const EXTERNAL_PG_URL = process.env.SD_TEST_PG_ADMIN_URL;
if (EXTERNAL_PG_URL && process.env.SD_TEST_PG_DISPOSABLE !== "yes") {
  throw new Error("SD_TEST_PG_ADMIN_URL runs migrations and needs SD_TEST_PG_DISPOSABLE=yes");
}
if ((!PG_BIN_DIR && !EXTERNAL_PG_URL) || !POSTGREST_BIN) {
  throw new Error("POSTGREST_BIN and either PG_BIN_DIR or SD_TEST_PG_ADMIN_URL must be set");
}

const WORKER = "session_directory_search_worker";
const ISSUER = "http://relayauth.relayauth.svc:8787/session-directory";
const AUDIENCE = "session-directory-search";
const RPC = "/rpc/search_session_directory";
const TERM = "quokka";

type Key = { kid: string; privateKey: KeyObject; publicKey: KeyObject };
const newKey = (kid: string): Key => ({ kid, ...generateKeyPairSync("rsa", { modulusLength: 2048 }) });
const SEARCH_KEY = newKey("search-current");
const NEXT_KEY = newKey("search-next");
const OTHER_TRUSTED_KEY = newKey("other-trusted");
const UNTRUSTED_KEY = newKey("untrusted");
const jwks = (...keys: Key[]) =>
  JSON.stringify({
    keys: keys.map((key) => ({ ...key.publicKey.export({ format: "jwk" }), kid: key.kid, alg: "RS256", use: "sig" })),
  });

const b64 = (value: unknown) => Buffer.from(JSON.stringify(value)).toString("base64url");
function sign(claims: Record<string, unknown>, key: Key = SEARCH_KEY, kid = key.kid): string {
  const body = `${b64({ alg: "RS256", typ: "JWT", kid })}.${b64(claims)}`;
  return `${body}.${createSign("RSA-SHA256").update(body).sign(key.privateKey).toString("base64url")}`;
}
const claimsFor = (org: string, workspace: string, overrides: Record<string, unknown> = {}) => {
  const iat = Math.floor(Date.now() / 1000);
  const claims: Record<string, unknown> = {
    iss: ISSUER,
    aud: AUDIENCE,
    sub: "user-1",
    role: WORKER,
    app_metadata: { org_id: org, workspace_ids: [workspace] },
    iat,
    nbf: iat,
    exp: iat + 60,
    jti: crypto.randomUUID(),
    ...overrides,
  };
  for (const [name, value] of Object.entries(claims)) if (value === undefined) delete claims[name];
  return claims;
};
const token = (org: string, workspace: string, overrides?: Record<string, unknown>) =>
  sign(claimsFor(org, workspace, overrides));

class Instance {
  proc!: Subprocess;
  constructor(readonly name: string, readonly port: number, readonly adminPort: number, readonly conf: string) {}
  async start() {
    this.proc = spawn([POSTGREST_BIN!, this.conf], { stdout: "ignore", stderr: "ignore" });
    await until(async () => (await fetch(`http://127.0.0.1:${this.adminPort}/ready`)).ok, `${this.name} ready`);
  }
  async reloadConfig(expectStatus: () => Promise<boolean>) {
    await admin.unsafe("NOTIFY pgrst, 'reload config'");
    await until(expectStatus, `${this.name} reload`);
    expect(this.proc.exitCode).toBeNull();
  }
  call(path: string, init: RequestInit = {}) {
    return fetch(`http://127.0.0.1:${this.port}${path}`, init);
  }
  search(bearer: string | null, workspace: string, query = TERM, path = RPC, headers: Record<string, string> = {}) {
    return this.call(path, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Content-Profile": "sessions",
        ...(bearer ? { Authorization: `Bearer ${bearer}` } : {}),
        ...headers,
      },
      body: JSON.stringify({ p_workspace_id: workspace, p_query: query }),
    });
  }
}

async function until(check: () => Promise<boolean>, label: string, timeoutMs = 15_000) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    try {
      if (await check()) return;
    } catch {}
    if (Date.now() > deadline) throw new Error(`timed out: ${label}`);
    await Bun.sleep(50);
  }
}
const freePort = () =>
  new Promise<number>((resolve) => {
    const server = createServer().listen(0, "127.0.0.1", () => {
      const { port } = server.address() as { port: number };
      server.close(() => resolve(port));
    });
  });
function loginUrl(adminUrl: string, role: string, password: string) {
  const url = new URL(adminUrl);
  url.username = role;
  url.password = password;
  return url.toString();
}
async function runOk(command: string[]) {
  const proc = spawn(command, { stdout: "pipe", stderr: "pipe" });
  if ((await proc.exited) !== 0) throw new Error(`${command[0]}: ${await new Response(proc.stderr).text()}`);
}

let dir: string;
let dataDir: string | undefined;
let admin: SQL;
let selfHosted: Instance;
let broad: Instance;
let selfHostedJwks: string;

type Row = Record<string, unknown>;
const rows = async (response: Response, status = 200) => {
  const text = await response.text();
  expect({ status: response.status, text: response.status === status ? "" : text }).toEqual({ status, text: "" });
  return JSON.parse(text) as Row[];
};
/** A refusal returns no rows and names no tenant data. */
async function refused(response: Response, statuses: number[], sqlstate?: string) {
  const text = await response.text();
  expect(statuses).toContain(response.status);
  expect(text).not.toContain(TERM);
  if (sqlstate) expect(JSON.parse(text).code).toBe(sqlstate);
}

async function insertRows(org: string, workspace: string, count: number, label: string) {
  for (let index = 0; index < count; index += 1) {
    await admin.unsafe(
      `INSERT INTO sessions.session_directory
         (org_id, workspace_id, harness, session_id, created_at, last_activity_at, title,
          agent_name, agent_name_normalized, project_id, search_text)
       VALUES ($1, $2, 'codex', $3, now(), now(), $4, 'Helm', 'helm', 'proj',
               sessions.session_directory_search_text($4, NULL, 'Helm', 'proj', NULL, NULL, NULL, NULL))`,
      [org, workspace, `${label}-${index}`, `${TERM} ${label}`],
    );
  }
}

beforeAll(async () => {
  dir = mkdtempSync(join(tmpdir(), "sd-pgrst-"));
  let adminUrl = EXTERNAL_PG_URL;
  if (!adminUrl) {
    dataDir = join(dir, "data");
    const pgPort = await freePort();
    await runOk([join(PG_BIN_DIR!, "initdb"), "-D", dataDir, "-U", "postgres", "--auth=trust", "-E", "UTF8", "--locale=C"]);
    await runOk([
      join(PG_BIN_DIR!, "pg_ctl"), "start", "-w", "-D", dataDir, "-l", join(dir, "pg.log"),
      "-o", `-p ${pgPort} -c listen_addresses=127.0.0.1 -k ${dir}`,
    ]);
    adminUrl = `postgres://postgres@127.0.0.1:${pgPort}/postgres`;
  }
  admin = new SQL(adminUrl);
  const [{ present }] = await admin.unsafe("SELECT to_regnamespace('sessions') IS NOT NULL AS present");
  if (present) throw new Error("refusing a database that already has a sessions schema");
  await admin.unsafe(read("supabase/tests/supabase_shim.sql")).simple();
  const migrations = readdirSync(join(root, "supabase/migrations")).sort();
  for (const file of migrations) await admin.unsafe(read(`supabase/migrations/${file}`)).simple();
  await admin.unsafe(read(`${PROFILE}/authenticator.sql`)).simple();
  await admin.unsafe(`
    CREATE ROLE broad_authenticator LOGIN NOINHERIT;
    GRANT anon, authenticated, service_role, ${WORKER} TO broad_authenticator;
  `).simple();
  await admin.unsafe(
    "INSERT INTO sessions.session_directory_search_token_policy (issuer, audience) VALUES ($1, $2)",
    [ISSUER, AUDIENCE],
  );
  const loginPassword = crypto.randomUUID().replaceAll("-", "");
  for (const role of ["session_directory_authenticator", "broad_authenticator"]) {
    await admin.unsafe(`ALTER ROLE ${role} PASSWORD '${loginPassword}'`);
  }

  await insertRows("org-a", "ws-a1", 3, "alpha");
  await insertRows("org-a", "ws-a2", 2, "alphatwo");
  await insertRows("org-a", "ws-shared", 2, "sharedalpha");
  await insertRows("org-b", "ws-b1", 3, "bravo");
  await insertRows("org-b", "ws-shared", 2, "sharedbravo");
  await insertRows("org-big", "ws-big", 60, "big");

  selfHostedJwks = join(dir, "selfhosted-jwks.json");
  const broadJwks = join(dir, "broad-jwks.json");
  writeFileSync(selfHostedJwks, jwks(SEARCH_KEY));
  writeFileSync(broadJwks, jwks(SEARCH_KEY, OTHER_TRUSTED_KEY));

  const profile = read(`${PROFILE}/postgrest.conf`);
  const profileConf = async (role: string, jwksPath: string, extra = "") => {
    const [port, adminPort] = [await freePort(), await freePort()];
    const conf = join(dir, `${role}.conf`);
    writeFileSync(
      conf,
      `${profile
        .replace("$(PGRST_DB_URI)", loginUrl(adminUrl, role, loginPassword))
        .replace("/etc/postgrest/session-directory-jwks.json", jwksPath)
        .replace("server-port = 3000", `server-port = ${port}`)
        .replace("admin-server-port = 3001", `admin-server-port = ${adminPort}`)}\ndb-pool = 2\n${extra}`,
    );
    return { port, adminPort, conf };
  };
  const hosted = await profileConf("session_directory_authenticator", selfHostedJwks);
  selfHosted = new Instance("selfHosted", hosted.port, hosted.adminPort, hosted.conf);
  const weak = await profileConf("broad_authenticator", broadJwks, 'db-anon-role = "anon"\n');
  writeFileSync(weak.conf, readFileSync(weak.conf, "utf8").replace(/^jwt-aud.*$/m, ""));
  broad = new Instance("broad", weak.port, weak.adminPort, weak.conf);
  await Promise.all([selfHosted.start(), broad.start()]);
}, 120_000);

afterAll(async () => {
  selfHosted?.proc?.kill();
  broad?.proc?.kill();
  await admin?.close();
  if (dataDir) await runOk([join(PG_BIN_DIR!, "pg_ctl"), "stop", "-m", "immediate", "-D", dataDir]).catch(() => {});
  if (dir) rmSync(dir, { recursive: true, force: true });
});

const instances = () => [selfHosted, broad];
const keysOf = (result: Row[]) => [...new Set(result.map((row) => `${row.org_id}/${row.workspace_id}`))];

describe("tenant isolation", () => {
  test("each tenant sees only its own rows for the same term", async () => {
    for (const instance of instances()) {
      const a = await rows(await instance.search(token("org-a", "ws-a1"), "ws-a1"));
      const b = await rows(await instance.search(token("org-b", "ws-b1"), "ws-b1"));
      expect(a).toHaveLength(3);
      expect(keysOf(a)).toEqual(["org-a/ws-a1"]);
      expect(b).toHaveLength(3);
      expect(keysOf(b)).toEqual(["org-b/ws-b1"]);
    }
  });

  test("the same workspace id in two orgs stays separated by org", async () => {
    const a = await rows(await selfHosted.search(token("org-a", "ws-shared"), "ws-shared"));
    const b = await rows(await selfHosted.search(token("org-b", "ws-shared"), "ws-shared"));
    expect(keysOf(a)).toEqual(["org-a/ws-shared"]);
    expect(keysOf(b)).toEqual(["org-b/ws-shared"]);
    expect(a.map((row) => row.session_id).sort()).toEqual(["sharedalpha-0", "sharedalpha-1"]);
  });

  test("a workspace outside the token is refused, in the same org and across orgs", async () => {
    await refused(await selfHosted.search(token("org-a", "ws-a1"), "ws-a2"), [403], "42501");
    await refused(await selfHosted.search(token("org-a", "ws-a1"), "ws-b1"), [403], "42501");
  });

  test("a multi-workspace claim searches only its set, and never another org", async () => {
    const bearer = token("org-a", "ws-a1", {
      app_metadata: { org_id: "org-a", workspace_ids: ["ws-a1", "ws-shared"] },
    });
    const claimed = await rows(await selfHosted.call(RPC, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Content-Profile": "sessions",
        Authorization: `Bearer ${bearer}`,
      },
      body: JSON.stringify({ p_query: TERM }),
    }));
    expect(keysOf(claimed).sort()).toEqual(["org-a/ws-a1", "org-a/ws-shared"]);
    await refused(await selfHosted.search(bearer, "ws-a2"), [403], "42501");
  });

  test("a token for an org that does not own the workspace returns nothing of it", async () => {
    expect(await rows(await selfHosted.search(token("org-a", "ws-b1"), "ws-b1"))).toEqual([]);
  });

  test("a tombstoned row disappears", async () => {
    await insertRows("org-c", "ws-c1", 2, "tomb");
    await admin.unsafe(
      "UPDATE sessions.session_directory SET deleted_at = now() WHERE org_id = 'org-c' AND session_id = 'tomb-0'",
    );
    const result = await rows(await selfHosted.search(token("org-c", "ws-c1"), "ws-c1"));
    expect(result.map((row) => row.session_id)).toEqual(["tomb-1"]);
  });
});

describe("forgery", () => {
  const good = () => claimsFor("org-a", "ws-a1");
  const bad: [string, () => string][] = [
    ["alg none", () => `${b64({ alg: "none", typ: "JWT" })}.${b64(good())}.`],
    ["HS256 signed with the public key", () => {
      const pem = SEARCH_KEY.publicKey.export({ type: "spki", format: "pem" }).toString();
      const body = `${b64({ alg: "HS256", typ: "JWT", kid: SEARCH_KEY.kid })}.${b64(good())}`;
      return `${body}.${createHmac("sha256", pem).update(body).digest("base64url")}`;
    }],
    ["a wrong key using a valid kid", () => sign(good(), UNTRUSTED_KEY, SEARCH_KEY.kid)],
    ["an unknown kid", () => sign(good(), UNTRUSTED_KEY)],
    ["a tampered payload", () => {
      const [header, , signature] = token("org-a", "ws-a1").split(".");
      return `${header}.${b64(claimsFor("org-b", "ws-b1"))}.${signature}`;
    }],
    ["expired", () => token("org-a", "ws-a1", { iat: Math.floor(Date.now() / 1000) - 300, exp: Math.floor(Date.now() / 1000) - 240 })],
    ["not yet valid", () => token("org-a", "ws-a1", { nbf: Math.floor(Date.now() / 1000) + 300 })],
  ];
  for (const [name, make] of bad) {
    test(`${name} is refused`, async () => {
      for (const instance of instances()) await refused(await instance.search(make(), "ws-a1"), [401]);
    });
  }

  test("a correctly shaped token from another key is refused by both layers", async () => {
    const claims = claimsFor("org-a", "ws-a1", { iss: "https://relayauth.dev" });
    await refused(await selfHosted.search(sign(claims, OTHER_TRUSTED_KEY), "ws-a1"), [401]);
    await refused(await broad.search(sign(claims, OTHER_TRUSTED_KEY), "ws-a1"), [403], "42501");
  });

  const pinned: [string, () => Record<string, unknown>][] = [
    ["wrong iss", () => ({ iss: "https://relayauth.dev" })],
    ["missing iss", () => ({ iss: undefined })],
    ["wrong aud", () => ({ aud: "relayhistory" })],
    ["array aud", () => ({ aud: [AUDIENCE] })],
    ["missing iat", () => ({ iat: undefined })],
    ["lifetime over 60s", () => ({ exp: Math.floor(Date.now() / 1000) + 61 })],
    ["duplicate workspaces", () => ({ app_metadata: { org_id: "org-a", workspace_ids: ["ws-a1", "ws-a1"] } })],
    ["65 workspaces", () => ({ app_metadata: { org_id: "org-a", workspace_ids: ["ws-a1", ...Array.from({ length: 64 }, (_, i) => `ws-${i}`)] } })],
    ["an empty workspace list", () => ({ app_metadata: { org_id: "org-a", workspace_ids: [] } })],
    ["a non-string workspace", () => ({ app_metadata: { org_id: "org-a", workspace_ids: ["ws-a1", 7] } })],
    ["an oversized workspace", () => ({ app_metadata: { org_id: "org-a", workspace_ids: ["ws-a1", "x".repeat(129)] } })],
    ["top-level tenant claims", () => ({ app_metadata: undefined, org_id: "org-a", workspace_ids: ["ws-a1"] })],
    ["string workspace_ids", () => ({ app_metadata: { org_id: "org-a", workspace_ids: "ws-a1" } })],
    ["array org_id", () => ({ app_metadata: { org_id: ["org-a"], workspace_ids: ["ws-a1"] } })],
  ];
  for (const [name, overrides] of pinned) {
    test(`the RPC refuses ${name} when PostgREST lets it through`, async () => {
      await refused(await broad.search(token("org-a", "ws-a1", overrides()), "ws-a1"), [403], "42501");
    });
  }

  test("a far-future iat is refused", async () => {
    const iat = Math.floor(Date.now() / 1000) + 3600;
    await refused(await broad.search(token("org-a", "ws-a1", { iat, nbf: undefined, exp: iat + 60 }), "ws-a1"), [401, 403]);
  });

  test("the self-hosted profile refuses a wrong audience before the RPC runs", async () => {
    await refused(await selfHosted.search(token("org-a", "ws-a1", { aud: "relayhistory" }), "ws-a1"), [401]);
  });

  test("an empty policy table refuses every token", async () => {
    await admin.unsafe("DELETE FROM sessions.session_directory_search_token_policy");
    try {
      await refused(await selfHosted.search(token("org-a", "ws-a1"), "ws-a1"), [403], "42501");
    } finally {
      await admin.unsafe(
        "INSERT INTO sessions.session_directory_search_token_policy (issuer, audience) VALUES ($1, $2)",
        [ISSUER, AUDIENCE],
      );
    }
    await rows(await selfHosted.search(token("org-a", "ws-a1"), "ws-a1"));
  });
});

describe("role boundary", () => {
  const table = (instance: Instance, bearer: string | null, path: string) =>
    instance.call(path, {
      headers: { "Accept-Profile": "sessions", ...(bearer ? { Authorization: `Bearer ${bearer}` } : {}) },
    });

  test("on the self-hosted profile, no other role can be assumed", async () => {
    for (const role of ["service_role", "authenticated", "anon", "postgres", "session_directory_authenticator", "no_such_role"]) {
      const bearer = token("org-a", "ws-a1", { role });
      await refused(await selfHosted.search(bearer, "ws-a1"), [401, 403]);
      await refused(await table(selfHosted, bearer, "/session_directory?select=title"), [401, 403]);
    }
  });

  test("on the broad instance, no other role can run the RPC", async () => {
    for (const role of ["service_role", "authenticated", "anon", "postgres", "broad_authenticator", "no_such_role"]) {
      await refused(await broad.search(token("org-a", "ws-a1", { role }), "ws-a1"), [401, 403]);
    }
  });

  test("no token is refused, and anon cannot run the RPC", async () => {
    await refused(await selfHosted.search(null, "ws-a1"), [401]);
    await refused(await broad.search(null, "ws-a1"), [401, 403]);
  });

  test("the Worker reads no table directly, including through embedding", async () => {
    const bearer = token("org-a", "ws-a1");
    for (const path of [
      "/session_directory?select=title",
      `/session_directory?search_text=ilike.*${TERM}*`,
      "/session_index?select=manifest_key",
      "/session_directory_users?select=user_id,session_directory(title)",
      "/session_directory_search_token_policy?select=issuer",
    ]) {
      await refused(await table(selfHosted, bearer, path), [403, 404], undefined);
    }
  });

  test("role attributes hold after the binding", async () => {
    const result = await admin.unsafe(
      "SELECT rolname, rolcanlogin, rolinherit, rolbypassrls FROM pg_roles WHERE rolname IN ($1, $2) ORDER BY rolname",
      ["session_directory_authenticator", WORKER],
    );
    expect(result.map((row: Row) => ({ ...row }))).toEqual([
      { rolname: "session_directory_authenticator", rolcanlogin: true, rolinherit: false, rolbypassrls: false },
      { rolname: WORKER, rolcanlogin: false, rolinherit: false, rolbypassrls: false },
    ]);
    const grants = await admin.unsafe(
      `SELECT r.rolname FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.roleid
        WHERE m.member = 'session_directory_authenticator'::regrole`,
    );
    expect(grants.map((row: Row) => row.rolname)).toEqual([WORKER]);
  });
});

describe("PostgREST surface", () => {
  test("query parameters and headers cannot widen the page or the columns", async () => {
    const bearer = token("org-big", "ws-big");
    expect(await rows(await selfHosted.search(bearer, "ws-big"))).toHaveLength(51);
    expect(await rows(await selfHosted.search(bearer, "ws-big", TERM, `${RPC}?limit=1000`))).toHaveLength(51);
    expect(await rows(await selfHosted.search(bearer, "ws-big", TERM, `${RPC}?offset=0&limit=1000&order=created_at.asc`))).toHaveLength(51);
    const prefer = await selfHosted.search(bearer, "ws-big", TERM, RPC, { Prefer: "count=exact, tx=rollback" });
    expect((await prefer.json()) as Row[]).toHaveLength(51);
    await refused(await selfHosted.search(bearer, "ws-big", TERM, `${RPC}?select=search_text`), [400]);
  });

  test("filters on the result cannot reach another tenant", async () => {
    const result = await rows(await selfHosted.search(token("org-a", "ws-a1"), "ws-a1", TERM, `${RPC}?org_id=eq.org-b`));
    expect(result).toEqual([]);
  });

  test("CSV output is bounded and tenant-scoped too", async () => {
    const response = await selfHosted.search(token("org-a", "ws-a1"), "ws-a1", TERM, RPC, { Accept: "text/csv" });
    const text = await response.text();
    expect(response.status).toBe(200);
    expect(text).toContain("org-a");
    expect(text).not.toContain("org-b");
  });

  test("a schema profile header cannot reach another schema", async () => {
    await refused(await selfHosted.search(token("org-a", "ws-a1"), "ws-a1", TERM, RPC, { "Content-Profile": "public" }), [406]);
    await refused(await selfHosted.search(token("org-a", "ws-a1"), "ws-a1", TERM, RPC, { "Content-Profile": "pg_catalog" }), [406]);
  });

  test("RPC errors keep their SQLSTATE and leak no data", async () => {
    await refused(await selfHosted.search(token("org-a", "ws-a1"), "ws-a1", "ab"), [400], "22023");
    await refused(await selfHosted.search(token("org-a", "ws-a1"), "ws-a2"), [403], "42501");
  });

  test("the 2s statement_timeout applies through PostgREST as 57014", async () => {
    const locker = await admin.reserve();
    await locker.unsafe("BEGIN");
    await locker.unsafe("LOCK TABLE sessions.session_directory IN ACCESS EXCLUSIVE MODE");
    try {
      const started = Date.now();
      const response = await selfHosted.search(token("org-a", "ws-a1"), "ws-a1");
      const elapsed = Date.now() - started;
      await refused(response, [500, 503, 504], "57014");
      expect(elapsed).toBeGreaterThan(1_500);
      expect(elapsed).toBeLessThan(5_000);
    } finally {
      await locker.unsafe("ROLLBACK");
      locker.release();
    }
  }, 15_000);
});

describe("concurrency and cache", () => {
  test("interleaved tenants over a small pool never see each other's rows", async () => {
    const tenants = [
      ["org-a", "ws-a1"], ["org-b", "ws-b1"], ["org-a", "ws-shared"], ["org-b", "ws-shared"], ["org-a", "ws-a2"],
    ] as const;
    const calls = Array.from({ length: 300 }, (_, index) => {
      const [org, workspace] = tenants[index % tenants.length]!;
      return selfHosted.search(token(org, workspace), workspace).then(async (response) => {
        expect(keysOf(await rows(response))).toEqual([`${org}/${workspace}`]);
      });
    });
    await Promise.all(calls);
  }, 30_000);

  test("claims do not survive on a reused connection", async () => {
    await rows(await selfHosted.search(token("org-a", "ws-a1"), "ws-a1"));
    await refused(await selfHosted.search(null, "ws-a1"), [401]);
    await refused(await broad.search(null, "ws-a1"), [401, 403]);
  });

  test("an expired token is refused even after PostgREST cached it", async () => {
    const now = Math.floor(Date.now() / 1000);
    const bearer = token("org-a", "ws-a1", { iat: now - 1, nbf: now - 1, exp: now + 1 });
    await rows(await selfHosted.search(bearer, "ws-a1"));
    // PostgREST allows 30 seconds of clock skew past exp.
    await Bun.sleep(33_000);
    await refused(await selfHosted.search(bearer, "ws-a1"), [401]);
  }, 45_000);
});

describe("rotation and kill switch", () => {
  const nextToken = () => sign(claimsFor("org-a", "ws-a1"), NEXT_KEY);
  const currentToken = () => sign(claimsFor("org-a", "ws-a1"), SEARCH_KEY);

  test("next, previous and removal follow the mounted JWKS", async () => {
    await refused(await selfHosted.search(nextToken(), "ws-a1"), [401]);
    writeFileSync(selfHostedJwks, jwks(SEARCH_KEY, NEXT_KEY));
    await selfHosted.reloadConfig(async () => (await selfHosted.search(nextToken(), "ws-a1")).status === 200);
    await rows(await selfHosted.search(currentToken(), "ws-a1"));

    writeFileSync(selfHostedJwks, jwks(NEXT_KEY));
    await selfHosted.reloadConfig(async () => (await selfHosted.search(currentToken(), "ws-a1")).status === 401);
    await rows(await selfHosted.search(nextToken(), "ws-a1"));

    writeFileSync(selfHostedJwks, jwks(SEARCH_KEY));
    await selfHosted.reloadConfig(async () => (await selfHosted.search(currentToken(), "ws-a1")).status === 200);
  }, 30_000);

  test("revoking the Worker from the login role stops search, and restoring recovers", async () => {
    const bearer = token("org-a", "ws-a1");
    await rows(await selfHosted.search(bearer, "ws-a1"));
    await admin.unsafe(`REVOKE ${WORKER} FROM session_directory_authenticator`);
    try {
      await refused(await selfHosted.search(token("org-a", "ws-a1"), "ws-a1"), [401, 403]);
    } finally {
      await admin.unsafe(`GRANT ${WORKER} TO session_directory_authenticator`);
    }
    await rows(await selfHosted.search(token("org-a", "ws-a1"), "ws-a1"));
  });
});

/**
 * Tokens from the real RelayAuth exchange, not fixtures. Needs RELAYAUTH_DIR: a built
 * checkout of relayauth with the session directory issuer. Its general key is a test
 * key, so this file can forge the subject tokens RelayAuth would have issued.
 */
// Resolved because the server only starts when argv[1] equals its real module path.
const RELAYAUTH_DIR = process.env.RELAYAUTH_DIR && realpathSync(process.env.RELAYAUTH_DIR);
describe.skipIf(!RELAYAUTH_DIR)("tokens minted by real RelayAuth", () => {
  const GENERAL_KEY = newKey("general");
  const GENERAL_ISSUER = "https://relayauth.test";
  let relayAuth: Subprocess;
  let baseUrl: string;
  let apiKey: string;
  let originalJwks: string;

  const generalToken = (claims: Record<string, unknown>) => {
    const iat = Math.floor(Date.now() / 1000);
    return sign(
      {
        iss: GENERAL_ISSUER, aud: ["relayhistory"], sub: "agent_reader", org: "relay_org-a", wks: "relay_ws-a1",
        scopes: ["rth:read"], sponsorId: "user-1", sponsorChain: ["user-1", "agent_reader"],
        token_type: "access", iat, exp: iat + 600, jti: crypto.randomUUID(), ...claims,
      },
      GENERAL_KEY,
    );
  };
  const exchange = (subjectToken: string, workspaceId: string) =>
    fetch(`${baseUrl}/v1/tokens/session-directory-search`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-api-key": apiKey },
      body: JSON.stringify({ subjectToken, workspaceId }),
    });
  const mint = async (org: string, workspace: string) => {
    const response = await exchange(generalToken({ org, wks: workspace }), workspace.replace(/^relay_/, ""));
    expect(response.status).toBe(201);
    return ((await response.json()) as { accessToken: string }).accessToken;
  };

  beforeAll(async () => {
    const port = await freePort();
    baseUrl = `http://127.0.0.1:${port}`;
    apiKey = `rak_${crypto.randomUUID().replaceAll("-", "")}`;
    const pem = (key: Key) => key.privateKey.export({ type: "pkcs8", format: "pem" }).toString();
    relayAuth = spawn(["node", "--import", "tsx", join(RELAYAUTH_DIR!, "packages/server/src/server.ts")], {
      cwd: RELAYAUTH_DIR!,
      stdout: "ignore",
      stderr: "ignore",
      env: {
        ...process.env,
        PORT: String(port),
        BASE_URL: baseUrl,
        RELAYAUTH_DB_PATH: join(dir, "relayauth.db"),
        INTERNAL_SECRET: crypto.randomUUID(),
        RELAYAUTH_ISSUER: GENERAL_ISSUER,
        RELAYAUTH_SIGNING_KEY_PEM: pem(GENERAL_KEY),
        RELAYAUTH_SESSION_DIRECTORY_BOOTSTRAP_API_KEY: apiKey,
        RELAYAUTH_SESSION_DIRECTORY_ISSUANCE: "true",
        RELAYAUTH_SESSION_DIRECTORY_ISSUER: ISSUER,
        RELAYAUTH_SESSION_DIRECTORY_SIGNING_KEY_PEM: pem(SEARCH_KEY),
      },
    });
    await until(async () => (await fetch(`${baseUrl}/.well-known/session-directory/jwks.json`)).ok, "relayauth ready", 30_000);

    // RelayAuth's kid is the key's thumbprint, so take it from the published set. Its
    // minting key comes from the boot setting, the only way to create the first one.
    const general = (await (await fetch(`${baseUrl}/.well-known/jwks.json`)).json()) as { keys: { kid: string }[] };
    GENERAL_KEY.kid = general.keys[0]!.kid;

    // PostgREST trusts exactly what RelayAuth publishes, with its real kids.
    originalJwks = readFileSync(selfHostedJwks, "utf8");
    const published = await (await fetch(`${baseUrl}/.well-known/session-directory/jwks.json`)).text();
    const previous = token("org-a", "ws-a1");
    writeFileSync(selfHostedJwks, published);
    await selfHosted.reloadConfig(async () => (await selfHosted.search(previous, "ws-a1")).status === 401);
  }, 60_000);

  afterAll(async () => {
    relayAuth?.kill();
    if (originalJwks) writeFileSync(selfHostedJwks, originalJwks);
  });

  test("a minted token reads its own tenant through PostgREST", async () => {
    const a = await rows(await selfHosted.search(await mint("relay_org-a", "relay_ws-a1"), "ws-a1"));
    const b = await rows(await selfHosted.search(await mint("relay_org-b", "relay_ws-b1"), "ws-b1"));
    expect(a).toHaveLength(3);
    expect(keysOf(a)).toEqual(["org-a/ws-a1"]);
    expect(keysOf(b)).toEqual(["org-b/ws-b1"]);
  });

  test("the same workspace id minted for two orgs stays separated", async () => {
    const a = await rows(await selfHosted.search(await mint("relay_org-a", "ws-shared"), "ws-shared"));
    const b = await rows(await selfHosted.search(await mint("relay_org-b", "ws-shared"), "ws-shared"));
    expect(keysOf(a)).toEqual(["org-a/ws-shared"]);
    expect(keysOf(b)).toEqual(["org-b/ws-shared"]);
  });

  test("a minted token cannot read another workspace", async () => {
    const bearer = await mint("relay_org-a", "relay_ws-a1");
    await refused(await selfHosted.search(bearer, "ws-a2"), [403], "42501");
    await refused(await selfHosted.search(bearer, "ws-b1"), [403], "42501");
  });

  test("the exchange refuses another tenant's workspace, so no token exists to try", async () => {
    const response = await exchange(generalToken({}), "ws-b1");
    expect(response.status).toBe(403);
    expect(((await response.json()) as { code: string }).code).toBe("workspace_mismatch");
  });

  test("a general RelayAuth token gets no access through PostgREST", async () => {
    await refused(await selfHosted.search(generalToken({ role: WORKER }), "ws-a1"), [401]);
  });
});
