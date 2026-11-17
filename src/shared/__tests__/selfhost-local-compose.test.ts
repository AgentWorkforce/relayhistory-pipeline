/**
 * Static checks of the local single-user compose (deploy/selfhost-local, relay#2204 S4;
 * docs/selfhost-local.md). Nothing here runs docker: the compose file is parsed, each
 * service's environment is interpolated with fixture values and fed to the same config
 * parsers the services call at startup, and nginx.conf is checked as text.
 */
import { describe, expect, test, beforeAll, afterAll } from "bun:test";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { selfhostIngestConfig } from "../../session-ingest/selfhost-config.ts";
import { readApiServiceConfig } from "../../session-read-api/service-config.ts";
import { readApiAuthConfig } from "../../session-read-api/lambda.ts";
import { assertLockerSelection } from "../locker-selection.ts";
import { loadSelfhostControlConfig, parseSelfhostControlUser } from "../../selfhost-control/server.ts";
import { TOKEN_ENV, tokensDocument, writeTokensFile } from "../../../deploy/selfhost-local/control-tokens.ts";
import { smokeBatch } from "../../../deploy/selfhost-local/smoke.ts";
import { deliveryAccount, parseDeliveryRequest } from "../../session-ingest/vendor/delivery/prepare.ts";

const ROOT = resolve(import.meta.dir, "../../..");
const DIR = join(ROOT, "deploy/selfhost-local");
const composeText = readFileSync(join(DIR, "compose.yaml"), "utf8");
const nginx = readFileSync(join(DIR, "nginx.conf"), "utf8");
const envExample = readFileSync(join(DIR, ".env.example"), "utf8");
type Service = {
  image?: string; environment?: Record<string, string>; volumes?: string[]; ports?: string[];
  entrypoint?: string[]; tmpfs?: string[]; deploy?: { replicas?: number }; depends_on?: Record<string, unknown>;
};
const compose = (Bun as unknown as { YAML: { parse(text: string): unknown } }).YAML.parse(composeText) as {
  name: string; services: Record<string, Service>; volumes: Record<string, unknown>;
};
const services = compose.services;

const SECRET_VARS = ["RH_PG_PASSWORD", "RH_INGEST_PASSWORD", "RH_READ_PASSWORD", "RH_CONSUMER_PASSWORD", "RH_INTROSPECT_SECRET", "RH_CONTROL_TOKEN"];
const fixture: Record<string, string> = {
  RH_ORIGIN: "https://localhost:8443",
  RH_PG_TLS_DIR: "/fixture/pg-tls",
  RH_EDGE_TLS_DIR: "/fixture/edge-tls",
  RH_PG_CA_SHA256: "0".repeat(64),
  RH_USER_SUB: "local-user",
  RH_USER_ORG: "local-org",
  RH_WORKSPACE: "local-workspace",
  RH_PG_PASSWORD: "a".repeat(64),
  RH_INGEST_PASSWORD: "b".repeat(64),
  RH_READ_PASSWORD: "c".repeat(64),
  RH_CONSUMER_PASSWORD: "d".repeat(64),
  RH_INTROSPECT_SECRET: "e".repeat(64),
  RH_CONTROL_TOKEN: `sh_at_${"f".repeat(64)}`,
};

/** Compose interpolation: ${VAR}, ${VAR:-default}, ${VAR:?message}; a :? var must be set and non-empty. */
function interpolate(value: string, vars: Record<string, string>): string {
  return value.replace(/\$\{([A-Z0-9_]+)(?:(:-|:\?)([^}]*))?\}/g, (_, name: string, op?: string, arg?: string) => {
    const v = vars[name];
    if (op === ":-") return v ? v : arg ?? "";
    if (op === ":?") { if (!v) throw new Error(`${name} required`); return v; }
    return v ?? "";
  });
}
const envOf = (name: string, vars = fixture) =>
  Object.fromEntries(Object.entries(services[name]!.environment ?? {}).map(([k, v]) => [k, interpolate(String(v), vars)])) as NodeJS.ProcessEnv;

let dir = "";
let vars: Record<string, string> = fixture;
beforeAll(() => {
  dir = mkdtempSync(join(tmpdir(), "selfhost-local-compose-"));
  const run = (...args: string[]) => execFileSync("openssl", args, { cwd: dir, stdio: "ignore" });
  run("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", "ca.key", "-out", "ca.crt", "-days", "2", "-subj", "/CN=fixture-ca",
    "-addext", "basicConstraints=critical,CA:TRUE");
  vars = { ...fixture, RH_PG_CA_SHA256: createHash("sha256").update(readFileSync(join(dir, "ca.crt"))).digest("hex") };
});
afterAll(() => { if (dir) rmSync(dir, { recursive: true, force: true }); });
/** The service env as the container sees it, with the CA path moved to the fixture file. */
const containerEnv = (name: string): NodeJS.ProcessEnv => {
  const env = envOf(name, vars);
  if (env.SESSION_POSTGRES_CA_FILE) {
    expect(env.SESSION_POSTGRES_CA_FILE).toBe("/certs/ca.crt");
    env.SESSION_POSTGRES_CA_FILE = join(dir, "ca.crt");
  }
  return env;
};

describe("selfhost-local compose: layout", () => {
  test("exactly the S4 services, each image-selected service runs a SERVICE the image entrypoint knows", () => {
    expect(compose.name).toBe("relayhistory-local");
    expect(Object.keys(services).sort()).toEqual([
      "consumer-bootstrap", "db", "local-init", "migrate", "proxy", "roles", "selfhost-control",
      "session-ingest", "session-pipeline-consumer", "session-read-api",
    ]);
    const entrypoint = readFileSync(join(ROOT, "docker/entrypoint.sh"), "utf8");
    const expected: Record<string, string> = {
      migrate: "hoopdb-migrate", "consumer-bootstrap": "session-pipeline-consumer-bootstrap",
      "selfhost-control": "selfhost-control", "session-ingest": "session-ingest",
      "session-pipeline-consumer": "session-pipeline-consumer", "session-read-api": "session-read-api",
    };
    for (const [name, svc] of Object.entries(services)) {
      if (svc.image !== "${RH_SERVICES_IMAGE:-relayhistory-services:local}") { expect(svc.environment?.SERVICE).toBeUndefined(); continue; }
      expect(svc.environment?.SERVICE).toBe(expected[name]!);
      expect(entrypoint).toContain(`\n  ${expected[name]})`);
    }
    expect(Object.keys(compose.volumes).sort()).toEqual(["edge-tls", "locker", "pgdata"]);
  });

  test("no pgvector, no MinIO, no hosted control plane; third-party images match the pins", () => {
    const code = composeText.split("\n").filter(line => !/^\s*#/.test(line)).join("\n");
    expect(code).not.toMatch(/vector|minio|relayauth|LOCKER_ENDPOINT|LOCKER_ACCESS_KEY_ID|LOCKER_SECRET_ACCESS_KEY|INGEST_LEDGER|INGEST_S3_|locker-init|SERVICE: (alleyoop|hoopdb-indexer|session-erase-bytes|session-publication)/i);
    const lock = JSON.parse(readFileSync(join(ROOT, "versions.lock.json"), "utf8")).images;
    const pinned = (key: string) => `${lock[key].ref.replace(/:[^:/]+$/, "")}:${lock[key].ref.split(":").pop()}@${lock[key].digest}`;
    expect(services["local-init"]!.image).toBe(pinned("init"));
    expect(services.proxy!.image).toBe(pinned("relayauth-tls-proxy"));
    for (const name of ["db", "roles"]) expect(services[name]!.image).toBe("postgres:17");
  });

  test("the only published port is the proxy's 8443 on loopback", () => {
    for (const [name, svc] of Object.entries(services)) if (name !== "proxy") expect(svc.ports).toBeUndefined();
    expect(services.proxy!.ports).toEqual(["127.0.0.1:${RH_HTTPS_PORT:-8443}:8443"]);
    expect(nginx).toMatch(/listen 8443 ssl;/);
  });

  test("listen ports line up with nginx upstreams and the control URL", () => {
    expect(envOf("session-ingest").RELAYHISTORY_INGEST_PORT).toBe("8081");
    expect(envOf("session-read-api").RELAYHISTORY_READ_API_PORT).toBe("8080");
    expect(envOf("selfhost-control").SELFHOST_CONTROL_LISTEN).toBe("0.0.0.0:8090");
    expect(nginx).toContain("set $read_api http://session-read-api:8080;");
    expect(nginx).toContain("set $ingest http://session-ingest:8081;");
    for (const name of ["session-ingest", "session-read-api"]) expect(envOf(name).RELAYCONTROL_INTROSPECT_URL).toBe("http://control.local.svc:8090");
    expect(composeText).toContain("aliases: [control.local.svc]");
  });

  test("fs Locker: one ingest replica writes it, the readers mount it read-only, init owns it for uid 1000", () => {
    for (const name of ["session-ingest", "session-read-api", "session-pipeline-consumer", "consumer-bootstrap"]) {
      const env = envOf(name);
      expect(env.LOCKER_PROVIDER).toBe("fs");
      expect(env.LOCKER_FS_ROOT).toBe("/locker");
    }
    expect(services["session-ingest"]!.deploy).toEqual({ replicas: 1 });
    expect(services["session-ingest"]!.volumes).toContain("locker:/locker");
    for (const name of ["session-read-api", "session-pipeline-consumer"]) expect(services[name]!.volumes).toContain("locker:/locker:ro");
    expect(services["local-init"]!.entrypoint!.join(" ")).toContain("chown 1000:1000 /locker && chmod 0700 /locker");
  });

  test("every service that reaches Postgres pins the CA (S1 custom-ca), never the hosted profile", () => {
    for (const name of ["migrate", "consumer-bootstrap", "session-ingest", "session-pipeline-consumer", "session-read-api"]) {
      const env = envOf(name, vars);
      expect(env.SESSION_POSTGRES_TLS_PROFILE).toBe("custom-ca");
      expect(env.SESSION_POSTGRES_CA_SHA256).toMatch(/^[a-f0-9]{64}$/);
      expect(services[name]!.volumes).toContain("${RH_PG_TLS_DIR:?}/ca.crt:/certs/ca.crt:ro");
    }
    expect(envOf("roles").PGSSLMODE).toBe("verify-full");
    expect(services.db!.entrypoint!.join(" ")).toContain("postgres -c ssl=on");
  });
});

describe("selfhost-local compose: the services' own config parsers accept it", () => {
  test("session-ingest (selfhost-config.ts): enabled, sql ledger, selection + heartbeat on, no erase fence", () => {
    const env = containerEnv("session-ingest");
    expect(() => assertLockerSelection(env)).not.toThrow();
    const config = selfhostIngestConfig(env);
    if (!config.enabled) throw new Error("ingest dormant");
    expect(config.port).toBe(8081);
    expect(config.origin).toBe("https://localhost:8443");
    expect(config.introspectUrl).toBe("http://control.local.svc:8090");
    expect(config.sessionSelection).toBe(true);
    expect(config.heartbeatAccept).toBe(true);
    expect(config.pool.user).toBe("bulls96_session_ingest");
    expect(config.pool.host).toBe("db");
    expect("ledger" in config).toBe(false);
    expect("s3EraseFence" in config).toBe(false);
    expect("admission" in config).toBe(false);
  });

  test("session-read-api: service config, relaycontrol_only auth, fs selection", () => {
    const env = containerEnv("session-read-api");
    expect(() => assertLockerSelection(env)).not.toThrow();
    const service = readApiServiceConfig(env);
    if (!service.enabled) throw new Error("read api dormant");
    expect(service.port).toBe(8080);
    expect(JSON.parse(service.secret).supabase_url).toStartWith("postgresql://bulls96_read_api:");
    const auth = readApiAuthConfig(env);
    expect(auth.mode).toBe("relaycontrol_only");
    expect(auth.origin).toBe("https://localhost:8443");
    expect(auth.relaycontrol?.url).toBe("http://control.local.svc:8090");
  });

  test("pipeline consumer and bootstrap: fs selection accepted", () => {
    for (const name of ["session-pipeline-consumer", "consumer-bootstrap"]) {
      const env = containerEnv(name);
      expect(env.RELAYHISTORY_SELFHOST_PIPELINE_CONSUMER).toBe("true");
      expect(() => assertLockerSelection(env)).not.toThrow();
      expect(JSON.parse(env.RELAYHISTORY_PIPELINE_CONSUMER_SECRET!).hoopdb_url).toStartWith("postgresql://");
    }
    expect(JSON.parse(envOf("session-pipeline-consumer").RELAYHISTORY_PIPELINE_CONSUMER_SECRET!).hoopdb_url).toStartWith("postgresql://search_consumer_runtime:");
  });

  test("selfhost-control: control-tokens.ts writes a tokens file the service loads", () => {
    const env = { ...envOf("selfhost-control"), SELFHOST_CONTROL_TOKENS_FILE: join(dir, "tokens.json") };
    expect(services["selfhost-control"]!.tmpfs).toEqual(["/run/selfhost-control:uid=1000,gid=1000,mode=0700"]);
    expect(envOf("selfhost-control").SELFHOST_CONTROL_TOKENS_FILE).toStartWith("/run/selfhost-control/");
    writeTokensFile(env);
    expect(statSync(env.SELFHOST_CONTROL_TOKENS_FILE).mode & 0o777).toBe(0o600);
    const config = loadSelfhostControlConfig(env);
    expect(config.secret).toBe(fixture.RH_INTROSPECT_SECRET!);
    expect(`${config.host}:${config.port}`).toBe("0.0.0.0:8090");
    const user = parseSelfhostControlUser(tokensDocument(env));
    expect(user.workspace).toBe("local-workspace");
    // The service process never inherits the raw token variable.
    expect(services["selfhost-control"]!.entrypoint!.join(" ")).toContain("exec env -u RH_CONTROL_TOKEN /entrypoint.sh");
    for (const name of TOKEN_ENV) expect(() => tokensDocument({ ...env, [name]: " " })).toThrow(`control-tokens: ${name} is required`);
    try { tokensDocument({ ...env, RH_CONTROL_TOKEN: "" }); } catch (error) { expect(String(error)).not.toContain(fixture.RH_CONTROL_TOKEN!); }
  });

  test("the services refuse to start when a required variable is unset (compose :? guard)", () => {
    for (const name of SECRET_VARS) {
      const missing = { ...vars, [name]: "" };
      const used = Object.keys(services).filter(s => JSON.stringify(services[s]!.environment ?? {}).includes(`\${${name}:?`));
      expect(used.length).toBeGreaterThan(0);
      for (const s of used) expect(() => envOf(s, missing)).toThrow(`${name} required`);
    }
  });
});

describe("selfhost-local compose: no secret literals", () => {
  const SECRETISH = /PASSWORD|SECRET|TOKEN|_KEY$|_URL$/;
  // Matched by name only: a URL with no credential, a file path, a feature flag.
  const NOT_SECRET = new Set(["RELAYCONTROL_INTROSPECT_URL", "SELFHOST_CONTROL_TOKENS_FILE", "RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS"]);
  test("every secret-bearing value is a required ${VAR:?} reference", () => {
    for (const [name, svc] of Object.entries(services)) for (const [key, raw] of Object.entries(svc.environment ?? {})) {
      const value = String(raw);
      if (!SECRETISH.test(key) || NOT_SECRET.has(key)) continue;
      expect(value).toMatch(/\$\{RH_[A-Z_]+:\?\}/);
      expect(value).not.toMatch(/sh_at_[A-Za-z0-9]/);
      // A connection URL's password is exactly one required reference, never a literal.
      for (const m of value.matchAll(/\/\/[^:/@]+:([^@]*)@/g)) expect(`${name}.${key}: ${m[1]}`).toMatch(/: \$\{RH_[A-Z]+_PASSWORD:\?\}$/);
      if (!/\/\//.test(value)) expect(`${name}.${key}: ${value}`).toMatch(/: \$\{RH_[A-Z_]+:\?\}$/);
    }
  });

  test("no literal that looks like a credential anywhere in the shipped files", () => {
    const files = ["compose.yaml", "nginx.conf", "roles.sh", "control-tokens.ts", "smoke.ts", ".env.example"];
    for (const file of files) {
      const text = readFileSync(join(DIR, file), "utf8");
      expect(text).not.toMatch(/sh_at_[A-Za-z0-9]{8,}|-----BEGIN [A-Z ]*PRIVATE KEY|AKIA[0-9A-Z]{16}|op:\/\/[A-Za-z]|\b[a-f0-9]{40,}\b(?<!sha256:[a-f0-9]{64})/);
    }
  });

  test(".env.example holds placeholders only: every secret empty, every compose variable listed", () => {
    const entries = new Map(envExample.split("\n").filter(l => /^[A-Z0-9_]+=/.test(l)).map(l => [l.slice(0, l.indexOf("=")), l.slice(l.indexOf("=") + 1)]));
    for (const name of [...SECRET_VARS, "RH_PG_CA_SHA256"]) expect(entries.get(name)).toBe("");
    const referenced = new Set([...composeText.matchAll(/\$\{(RH_[A-Z0-9_]+)/g)].map(m => m[1]!));
    for (const name of referenced) expect(entries.has(name) || envExample.includes(`# ${name}=`)).toBe(true);
    for (const name of entries.keys()) expect(referenced.has(name)).toBe(true);
  });

  test("the real env file is gitignored and the template is not", () => {
    const check = (path: string) => { try { execFileSync("git", ["check-ignore", "-q", path], { cwd: ROOT }); return true; } catch { return false; } };
    expect(check("deploy/selfhost-local/.env")).toBe(true);
    expect(check("deploy/selfhost-local/.env.example")).toBe(false);
    expect(check(".96bulls/pg-tls/server.key")).toBe(true);
  });
});

describe("selfhost-local nginx.conf", () => {
  const locations = [...nginx.matchAll(/location (=|\^~) (\S+) \{\s*proxy_pass \$(read_api|ingest);\s*\}/g)].map(m => `${m[1]} ${m[2]} -> ${m[3]}`);
  test("routes exactly the docs/selfhost-composition.md section 2 table, everything else 404", () => {
    expect(locations.sort()).toEqual([
      "= /v1/auth/token/refresh -> ingest",
      "= /v1/cli/login -> read_api",
      "= /v1/delivery/records -> read_api",
      "= /v1/events -> read_api",
      "= /v1/sessions -> read_api",
      "^~ /v1/delivery/ -> ingest",
      "^~ /v1/digest/ -> read_api",
      "^~ /v1/onboarding/ -> ingest",
      "^~ /v1/sessions/ -> read_api",
      "^~ /v1/turns/ -> ingest",
    ]);
    expect(nginx).toMatch(/location \/ \{\s*return 404;\s*\}/);
    expect(nginx.match(/\blocation\b/g)!.length).toBe(locations.length + 1);
    // proxy_pass with a variable and no URI part forwards the raw request URI, never a rewritten one.
    expect(nginx).not.toMatch(/proxy_pass \$\w+\//);
    expect(nginx).not.toMatch(/location[^{]*(internal|healthz|readyz)/);
  });

  test("TLS edge, client identity headers stripped, body cap, raw-path guard", () => {
    expect(nginx).toMatch(/ssl_certificate\s+\/etc\/nginx\/tls\/server\.crt;/);
    expect(nginx).toMatch(/ssl_certificate_key\s+\/etc\/nginx\/tls\/server\.key;/);
    expect(nginx).toMatch(/ssl_protocols TLSv1\.2 TLSv1\.3;/);
    expect(nginx).toContain('proxy_set_header x-relay-ingest-identity "";');
    expect(nginx).toContain('proxy_set_header x-relay-ingest-signature "";');
    expect(nginx).toContain("proxy_set_header X-Forwarded-Proto https;");
    expect(nginx).toMatch(/client_max_body_size 4m;/);
    expect(nginx).toContain("server_tokens off;");
    // nginx's quoted-string unescaping turns \\\\ into the regex \\ (one literal backslash).
    const guard = nginx.match(/if \(\$request_uri ~ "([^"]+)"\) \{\s*return 404;\s*\}/);
    expect(guard).not.toBeNull();
    const re = new RegExp(guard![1]!.replace(/\\\\/g, "\\"));
    for (const bad of ["/v1/delivery/..\\..\\internal\\x", "/v1/delivery/%5c..", "/v1/sessions/%2e%2e/internal", "/v1/delivery/%2Finternal",
      "//internal", "/v1/sessions/../internal", "/v1/sessions/./x", "/v1/sessions/..", "/v1/sessions/..?a=1"]) expect(re.test(bad)).toBe(true);
    for (const ok of ["/v1/sessions", "/v1/sessions/abc/turns?cursor=a%2Fb", "/v1/delivery/records?q=a//b%5c", "/v1/sessions/a.b..c"]) expect(re.test(ok)).toBe(false);
  });
});

describe("selfhost-local smoke", () => {
  test("the smoke batch passes the ingest's own delivery parser, for the configured account", async () => {
    const user = { sub: "local-user", org: "local-org", workspace: "local-workspace" };
    const { sessionId, body } = await smokeBatch(user, "marker-1", Date.parse("2026-11-17T00:00:00.000Z"));
    const batch = parseDeliveryRequest(body);
    expect(batch.account_id).toBe(await deliveryAccount({ orgId: "local-org", workspaceId: "local-workspace" }));
    expect(batch.records.map(r => r.kind)).toEqual(["session", "history", "session_event", "session_event"]);
    for (const record of batch.records) expect(record.session_id).toBe(sessionId);
    expect(JSON.stringify(body)).toContain("marker-1");
  });
});
