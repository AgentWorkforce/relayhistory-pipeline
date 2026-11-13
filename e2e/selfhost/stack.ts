/**
 * Generates the per-run credentials and CA, and drives `docker compose` for the
 * self-host acceptance stack. No credential is printed or written outside the
 * run's own temporary directory, which is removed on exit.
 */
import { execFileSync, spawnSync } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

export const here = fileURLToPath(new URL(".", import.meta.url));
const root = join(here, "../..");
const rand = () => randomBytes(18).toString("hex");

export interface Tenant { sub: string; org: string; workspace: string }
export const TENANT_A: Tenant = { sub: "user_a", org: "org_a", workspace: "ws_a" };
export const TENANT_B: Tenant = { sub: "user_b", org: "org_b", workspace: "ws_b" };

export interface StackSession {
  env: NodeJS.ProcessEnv;
  ingestUrl: string;
  readUrl: string;
  tokens: { a: string; b: string; aReadOnly: string };
  compose(args: string[], options?: { allowFail?: boolean; quiet?: boolean }): { status: number; stdout: string };
  /** Host ports are ephemeral and change when a container restarts. */
  resolveUrls(): void;
  cleanup(): void;
}

function makeCerts(dir: string): string {
  const openssl = (...args: string[]) => execFileSync("openssl", args, { stdio: "ignore" });
  openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2", "-subj", "/CN=e2e-ca",
    "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign",
    "-keyout", join(dir, "ca.key"), "-out", join(dir, "ca.crt"));
  openssl("req", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=db", "-keyout", join(dir, "server.key"), "-out", join(dir, "server.csr"));
  const ext = join(dir, "ext.cnf");
  writeFileSync(ext, "subjectAltName=DNS:db\n");
  execFileSync("sh", ["-c", `openssl x509 -req -in "${dir}/server.csr" -CA "${dir}/ca.crt" -CAkey "${dir}/ca.key" -CAcreateserial -days 2 -extfile "${ext}" -out "${dir}/server.crt"`], { stdio: "ignore" });
  rmSync(join(dir, "ca.key"));
  rmSync(join(dir, "server.csr"));
  rmSync(ext);
  for (const f of ["ca.crt", "server.crt", "server.key"]) chmodSync(join(dir, f), 0o644);
  return createHash("sha256").update(readFileSync(join(dir, "ca.crt"))).digest("hex");
}

/**
 * `files` layers more compose files over compose.yaml and `project` names the compose project (default the
 * file's own `rh-e2e`), so another lane (e2e/pipeline) runs the same stack, extended, beside this one.
 * `extraEnv` adds per-run values those files read.
 */
export interface StackOptions { files?: string[]; project?: string; extraEnv?: Record<string, string> }

export function createStack(options: StackOptions = {}): StackSession {
  mkdirSync(join(root, ".96bulls"), { recursive: true });
  const certs = mkdtempSync(join(root, ".96bulls/e2e-"));
  chmodSync(certs, 0o755);
  const caSha = makeCerts(certs);
  const tokens = { a: `sh_at_${rand()}`, b: `sh_at_${rand()}`, aReadOnly: `sh_at_${rand()}` };
  const tokenTable: Record<string, Tenant & { scopes: string[] }> = {
    [tokens.a]: { ...TENANT_A, scopes: ["rth:read", "rth:sync"] },
    [tokens.b]: { ...TENANT_B, scopes: ["rth:read", "rth:sync"] },
    [tokens.aReadOnly]: { ...TENANT_A, scopes: ["rth:read"] },
  };
  const env: NodeJS.ProcessEnv = {
    ...process.env,
    E2E_CERTS_DIR: certs,
    E2E_CA_SHA256: caSha,
    E2E_PG_PASSWORD: rand(),
    E2E_MINIO_USER: `m${rand()}`,
    E2E_MINIO_PASSWORD: rand(),
    E2E_READ_PASSWORD: rand(),
    E2E_INGEST_PASSWORD: rand(),
    E2E_CONSUMER_PASSWORD: rand(),
    E2E_INTROSPECT_SECRET: rand(),
    E2E_TOKENS: JSON.stringify(tokenTable),
    ...options.extraEnv,
  };
  const files = [join(here, "compose.yaml"), ...(options.files ?? [])].flatMap((f) => ["-f", f]);
  const project = options.project ? ["-p", options.project] : [];
  const compose: StackSession["compose"] = (args, options) => {
    const r = spawnSync("docker", ["compose", ...project, ...files, ...args], { env, encoding: "utf8", maxBuffer: 64 * 1024 * 1024, stdio: ["ignore", "pipe", options?.quiet ? "pipe" : "inherit"] });
    if (r.status !== 0 && !options?.allowFail) throw new Error(`docker compose ${args[0]} failed (${r.status})`);
    return { status: r.status ?? 1, stdout: r.stdout ?? "" };
  };
  const hostPort = (service: string, port: string) => {
    const out = compose(["port", service, port], { quiet: true, allowFail: true }).stdout.trim();
    const value = out.slice(out.lastIndexOf(":") + 1);
    if (!/^\d+$/u.test(value)) throw new Error(`no host port for ${service}:${port}`);
    return value;
  };
  return {
    env,
    ingestUrl: "",
    readUrl: "",
    tokens,
    compose,
    resolveUrls() {
      this.ingestUrl = `http://127.0.0.1:${hostPort("session-ingest", "8081")}`;
      this.readUrl = `http://127.0.0.1:${hostPort("session-read-api", "8080")}`;
    },
    cleanup() {
      rmSync(certs, { recursive: true, force: true });
    },
  };
}

export function buildImage(): void {
  // E2E_DOCKER_BUILD_NETWORK=host: for a Docker whose build containers have no DNS of their own (the Blaxel
  // docker-in-sandbox image; e2e/pipeline/sandbox-run.sh sets it).
  const network = process.env.E2E_DOCKER_BUILD_NETWORK === "host" ? ["--network", "host"] : [];
  execFileSync("docker", ["build", ...network, "-q", "-f", join(root, "docker/services.Dockerfile"), "-t", "96bulls/services:e2e", root], { stdio: ["ignore", "ignore", "inherit"] });
}
