/**
 * Static checks of the local single-user compose (deploy/selfhost-local, relay#2204 S4;
 * docs/selfhost-local.md). Nothing here runs docker: the compose file is parsed, each
 * service's environment is interpolated with fixture values and fed to the same config
 * parsers the services call at startup, and nginx.conf is checked as text.
 */
import { describe, expect, test, beforeAll, afterAll } from "bun:test";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtempSync, readdirSync, readFileSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { selfhostIngestConfig } from "../../session-ingest/selfhost-config.ts";
import { readApiServiceConfig } from "../../session-read-api/service-config.ts";
import { readApiAuthConfig } from "../../session-read-api/lambda.ts";
import { assertLockerSelection } from "../locker-selection.ts";
import { consumerServiceConfig } from "../../session-pipeline/consumer-config.ts";
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
  network_mode?: string;
};
const compose = (Bun as unknown as { YAML: { parse(text: string): unknown } }).YAML.parse(composeText) as {
  name: string; services: Record<string, Service>; volumes: Record<string, unknown>;
};
const services = compose.services;
/** A deep copy of the services to seed a fault into; the checks below must then report it. */
const mutated = (seed: (copy: Record<string, Service>) => void): Record<string, Service> => {
  const copy = structuredClone(services);
  seed(copy);
  return copy;
};

/**
 * Every environment variable name the shipped code reads: a quoted literal or an `env.NAME` access in
 * TS/JS, or a `$NAME` / `${NAME` in a shell script, under src/, packages/ and docker/ (tests excluded).
 */
const SOURCE_NAMES: Set<string> = (() => {
  const names = new Set<string>();
  const walk = (path: string) => {
    for (const entry of readdirSync(path, { withFileTypes: true })) {
      const full = join(path, entry.name);
      if (entry.isDirectory()) {
        if (entry.name !== "node_modules" && entry.name !== "__tests__" && entry.name !== "dist") walk(full);
        continue;
      }
      if (/\.test\.[cm]?[jt]sx?$/.test(entry.name)) continue;
      const text = () => readFileSync(full, "utf8");
      if (/\.[cm]?[jt]sx?$/.test(entry.name)) {
        for (const m of text().matchAll(/["'`]([A-Z][A-Z0-9_]+)["'`]|\benv\??\.([A-Z][A-Z0-9_]+)\b/g)) names.add(m[1] ?? m[2]!);
      } else if (entry.name.endsWith(".sh")) {
        for (const m of text().matchAll(/\$\{?([A-Z][A-Z0-9_]+)/g)) names.add(m[1]!);
      }
    }
  };
  for (const top of ["src", "packages", "docker"]) walk(join(ROOT, top));
  return names;
})();
/** Keys no src/packages/docker code reads: each names the deploy script (or image) that reads it, and the only service that may set it. */
const DEPLOY_SCRIPT_KEYS: Record<string, { file: string; service: string }> = {
  RH_USER_SUB: { file: "control-tokens.ts", service: "selfhost-control" },
  RH_USER_ORG: { file: "control-tokens.ts", service: "selfhost-control" },
  RH_WORKSPACE: { file: "control-tokens.ts", service: "selfhost-control" },
  RH_CONTROL_TOKEN: { file: "control-tokens.ts", service: "selfhost-control" },
  RH_READ_PASSWORD: { file: "roles.sh", service: "roles" },
  RH_INGEST_PASSWORD: { file: "roles.sh", service: "roles" },
  RH_CONSUMER_PASSWORD: { file: "roles.sh", service: "roles" },
};
/** libpq's own environment (the psql that roles.sh runs reads them); only the `roles` service sets them. */
const LIBPQ_KEYS = new Set(["PGHOST", "PGUSER", "PGDATABASE", "PGPASSWORD", "PGSSLMODE", "PGSSLROOTCERT"]);
/** Read by a third-party image's own entrypoint: the postgres image initialises the superuser from it. */
const IMAGE_KEYS: Record<string, string> = { POSTGRES_PASSWORD: "db" };
/**
 * Compose interpolation variables that never reach a container environment, with what they feed.
 * Every other ${VAR} in compose.yaml must appear inside some service's environment.
 */
const INTERPOLATION_ONLY: Record<string, string> = {
  RH_HTTPS_PORT: "the proxy's published loopback port",
  RH_SERVICES_IMAGE: "the services image tag",
  RH_PG_TLS_DIR: "bind-mount source of the Postgres certificates",
  RH_EDGE_TLS_DIR: "bind-mount source of the edge certificate",
};
function envKeyProblems(svcs: Record<string, Service>): string[] {
  const problems: string[] = [];
  for (const [name, svc] of Object.entries(svcs)) for (const key of Object.keys(svc.environment ?? {})) {
    const script = DEPLOY_SCRIPT_KEYS[key];
    if (script) {
      if (name !== script.service) problems.push(`${name}.${key}: only ${script.service} may set it`);
      if (!readFileSync(join(DIR, script.file), "utf8").includes(key)) problems.push(`${name}.${key}: not read by ${script.file}`);
    } else if (LIBPQ_KEYS.has(key)) {
      if (name !== "roles" || !(svc.entrypoint ?? []).includes("/deploy/roles.sh")) problems.push(`${name}.${key}: libpq key outside roles.sh`);
    } else if (IMAGE_KEYS[key] !== undefined) {
      if (IMAGE_KEYS[key] !== name) problems.push(`${name}.${key}: image key on the wrong service`);
    } else if (!SOURCE_NAMES.has(key)) problems.push(`${name}.${key}: no code reads it`);
  }
  return problems;
}
function interpolationProblems(svcs: Record<string, Service>, text: string): string[] {
  const inEnv = JSON.stringify(Object.values(svcs).map(s => s.environment ?? {}));
  const code = text.split("\n").filter(line => !/^\s*#/.test(line)).join("\n");
  const referenced = new Set([...code.matchAll(/\$\{([A-Z0-9_]+)/g)].map(m => m[1]!));
  const problems: string[] = [];
  for (const name of referenced) {
    const used = inEnv.includes(`\${${name}`);
    if (INTERPOLATION_ONLY[name] !== undefined && used) problems.push(`${name}: listed interpolation-only but reaches an environment`);
    if (INTERPOLATION_ONLY[name] === undefined && !used) problems.push(`${name}: referenced but not in any environment nor INTERPOLATION_ONLY`);
  }
  for (const name of Object.keys(INTERPOLATION_ONLY)) if (!referenced.has(name)) problems.push(`${name}: in INTERPOLATION_ONLY, never referenced`);
  return problems;
}
/** The fs Locker mounts: exactly one writer (ingest), the two readers read-only, nobody else. */
const LOCKER_MOUNTS: Record<string, string[]> = {
  "session-ingest": ["locker:/locker"],
  "session-read-api": ["locker:/locker:ro"],
  "session-pipeline-consumer": ["locker:/locker:ro"],
  "local-init": ["locker:/locker"],
};
const lockerMounts = (svcs: Record<string, Service>) =>
  Object.fromEntries(Object.entries(svcs).map(([n, s]) => [n, (s.volumes ?? []).filter(v => v.startsWith("locker:"))]).filter(([, v]) => v!.length > 0));
/** Host exposure: no service shares a host or container network namespace; one loopback port, the proxy's. */
function exposureProblems(svcs: Record<string, Service>): string[] {
  const problems: string[] = [];
  for (const [name, svc] of Object.entries(svcs)) {
    if (svc.network_mode !== undefined) problems.push(`${name}: network_mode ${svc.network_mode}`);
    for (const port of svc.ports ?? []) if (name !== "proxy" || port !== "127.0.0.1:${RH_HTTPS_PORT:-8443}:8443") problems.push(`${name}: port ${port}`);
  }
  if (JSON.stringify(svcs.proxy?.ports) !== JSON.stringify(["127.0.0.1:${RH_HTTPS_PORT:-8443}:8443"])) problems.push("proxy: published ports");
  return problems;
}

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

  test("the only published port is the proxy's 8443 on loopback, and no service sets network_mode", () => {
    for (const [name, svc] of Object.entries(services)) if (name !== "proxy") expect(svc.ports).toBeUndefined();
    expect(services.proxy!.ports).toEqual(["127.0.0.1:${RH_HTTPS_PORT:-8443}:8443"]);
    expect(exposureProblems(services)).toEqual([]);
    // Text-level too, so a key the typed parse would miss still counts.
    expect(composeText).not.toMatch(/network_mode/);
    expect(composeText.match(/^\s*ports:/gm)!.length).toBe(1);
    expect(nginx).toMatch(/listen 8443 ssl;/);
    // Seeded faults.
    expect(exposureProblems(mutated(c => { c.db!.network_mode = "host"; }))).toEqual(["db: network_mode host"]);
    expect(exposureProblems(mutated(c => { c["session-read-api"]!.ports = ["8080:8080"]; }))).toEqual(["session-read-api: port 8080:8080"]);
    expect(exposureProblems(mutated(c => { c.proxy!.ports = ["${RH_HTTPS_PORT:-8443}:8443"]; })).length).toBeGreaterThan(0);
    expect(exposureProblems(mutated(c => { c.proxy!.ports!.push("127.0.0.1:9000:9000"); })).length).toBeGreaterThan(0);
  });

  test("every environment key the compose sets is read by the code (or an allowlisted script/image), so a typo fails", () => {
    expect(SOURCE_NAMES.size).toBeGreaterThan(100);
    expect(envKeyProblems(services)).toEqual([]);
    expect(interpolationProblems(services, composeText)).toEqual([]);
    for (const [key, { file }] of Object.entries(DEPLOY_SCRIPT_KEYS)) {
      expect(readFileSync(join(DIR, file), "utf8")).toContain(key);
      expect(SOURCE_NAMES.has(key)).toBe(false); // otherwise it belongs in no allowlist
    }
    expect(readFileSync(join(DIR, "roles.sh"), "utf8")).toMatch(/command psql /);
    // Seeded faults: a misspelled key, a script key moved to another service, a libpq key outside roles.
    const rename = (svc: string, from: string, to: string) => mutated(c => {
      const env = c[svc]!.environment!;
      env[to] = env[from]!;
      delete env[from];
    });
    expect(envKeyProblems(rename("session-read-api", "READ_API_TURNS_ASSUME_NO_LEGACY", "READ_API_TURNS_ASUME_NO_LEGACY")))
      .toEqual(["session-read-api.READ_API_TURNS_ASUME_NO_LEGACY: no code reads it"]);
    expect(envKeyProblems(rename("session-ingest", "RELAYHISTORY_SELFHOST_HEARTBEAT_ACCEPT", "RELAYHISTORY_SELFHOST_HEARTBEAT_ACCEPTS"))).toHaveLength(1);
    expect(envKeyProblems(rename("session-pipeline-consumer", "CONSUMER_RECONCILE_EVERY_MINUTES", "CONSUMER_RECONCILE_EVERY_MINUTE"))).toHaveLength(1);
    expect(envKeyProblems(mutated(c => { c["session-ingest"]!.environment!.RH_CONTROL_TOKEN = "x"; })))
      .toEqual(["session-ingest.RH_CONTROL_TOKEN: only selfhost-control may set it"]);
    expect(envKeyProblems(mutated(c => { c["session-read-api"]!.environment!.PGPASSWORD = "x"; }))).toHaveLength(1);
    expect(envKeyProblems(mutated(c => { c.roles!.environment!.POSTGRES_PASSWORD = "x"; }))).toHaveLength(1);
    expect(interpolationProblems(services, composeText.replace("${RH_HTTPS_PORT:-8443}", "${RH_HTTPS_PROT:-8443}")))
      .toEqual(["RH_HTTPS_PROT: referenced but not in any environment nor INTERPOLATION_ONLY", "RH_HTTPS_PORT: in INTERPOLATION_ONLY, never referenced"]);
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
    expect(services["session-ingest"]!.volumes!.filter(v => v.startsWith("locker:"))).toEqual(["locker:/locker"]);
    for (const name of ["session-read-api", "session-pipeline-consumer"]) expect(services[name]!.volumes!.filter(v => v.startsWith("locker:"))).toEqual(["locker:/locker:ro"]);
    expect(lockerMounts(services)).toEqual(LOCKER_MOUNTS);
    // Seeded faults: a reader that can write, a second writer mount, a new service on the Locker.
    expect(lockerMounts(mutated(c => { c["session-read-api"]!.volumes = c["session-read-api"]!.volumes!.map(v => v.replace("locker:/locker:ro", "locker:/locker")); }))).not.toEqual(LOCKER_MOUNTS);
    expect(lockerMounts(mutated(c => { c["session-pipeline-consumer"]!.volumes!.push("locker:/locker2"); }))).not.toEqual(LOCKER_MOUNTS);
    expect(lockerMounts(mutated(c => { c["consumer-bootstrap"]!.volumes!.push("locker:/locker:ro"); }))).not.toEqual(LOCKER_MOUNTS);
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

  test("pipeline consumer: its own config parser (consumer-config.ts) accepts the env", () => {
    const env = containerEnv("session-pipeline-consumer");
    const config = consumerServiceConfig(env);
    if (!config.enabled) throw new Error("consumer dormant");
    expect(config.searchBackend).toBe("postgres");
    expect(config.backendId).toBe("local-postgres");
    expect(config.keyPrefix).toBe("96bulls");
    expect(config.everyMinutes).toBe(1);
    expect(config.lookbackMinutes).toBe(30);
    expect(config.floorMs).toBe(0);
    expect(config.secret.hoopdb_url).toStartWith("postgresql://search_consumer_runtime:");
    // Seeded faults the parser must refuse.
    expect(() => consumerServiceConfig({ ...env, CONSUMER_RECONCILE_EVERY_MINUTES: "30" })).toThrow("LOOKBACK_MINUTES must be greater");
    expect(() => consumerServiceConfig({ ...env, SEARCH_INDEX_BACKEND: "pgvector" })).toThrow("SEARCH_INDEX_BACKEND");
    expect(() => consumerServiceConfig({ ...env, PIPELINE_KEY_PREFIX: "" })).toThrow("PIPELINE_KEY_PREFIX");
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
