/**
 * Local Postgres TLS for a single-user self-host: mint a throwaway CA and a
 * Postgres server certificate signed by it, for SESSION_POSTGRES_TLS_PROFILE=custom-ca
 * (src/shared/postgres-tls.ts; docs/local-stack.md "Local Postgres TLS in two commands").
 *
 *   bun scripts/selfhost-local-ca.ts --out .96bulls/pg-tls [--host db --host localhost --host 127.0.0.1] [--days 825]
 *
 * Writes ca.crt (0644), server.crt (0644) and server.key (0600) into --out, which
 * must not exist or must be an empty directory. The CA private key never leaves a
 * private temporary directory and is deleted on exit, so the CA can sign nothing
 * else; rerun into a new directory to rotate. Prints exactly one line,
 * SESSION_POSTGRES_CA_SHA256=<hex>, the sha256 of the ca.crt bytes, which is the
 * value postgres-tls.ts pins. Key material is never printed.
 *
 * Same openssl CLI approach as e2e/selfhost/stack.ts (node:crypto cannot issue
 * certificates); the minimal logic is copied rather than shared because that
 * harness wants a 2-day, db-only, per-run CA.
 */
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { chmodSync, lstatSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { isIP } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

export const DEFAULT_HOSTS = ["db", "localhost", "127.0.0.1"] as const;
export const DEFAULT_DAYS = 825;
const DNS_NAME = /^(?=.{1,253}$)[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*$/;

export interface LocalCaOptions { out: string; hosts: string[]; days: number }

export class LocalCaError extends Error {
  constructor(message: string) { super(message); this.name = "LocalCaError"; }
}

export function parseLocalCaArgs(argv: readonly string[]): LocalCaOptions {
  let out = "";
  const hosts: string[] = [];
  let days = DEFAULT_DAYS;
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    const value = argv[i + 1];
    if (arg === "--out" || arg === "--host" || arg === "--days") {
      if (value === undefined || value.startsWith("--")) throw new LocalCaError(`${arg} needs a value`);
      i++;
      if (arg === "--out") out = value;
      else if (arg === "--host") hosts.push(value);
      else days = /^\d+$/.test(value) ? Number(value) : NaN;
    } else throw new LocalCaError(`unknown argument ${arg}`);
  }
  if (!out) throw new LocalCaError("--out <dir> is required");
  if (!Number.isInteger(days) || days < 1 || days > 3650) throw new LocalCaError("--days must be an integer in 1..3650");
  const names = hosts.length ? hosts : [...DEFAULT_HOSTS];
  for (const host of names) if (!isIP(host) && !DNS_NAME.test(host)) throw new LocalCaError(`--host ${JSON.stringify(host)} is not a DNS name or IP address`);
  return { out, hosts: [...new Set(names)], days };
}

/** Refuses anything but a missing path or an empty real directory (no symlinks). */
function prepareOut(out: string): void {
  let stat;
  try { stat = lstatSync(out); } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    mkdirSync(out, { recursive: true, mode: 0o755 });
    return;
  }
  if (!stat.isDirectory()) throw new LocalCaError(`--out ${out} exists and is not a directory`);
  if (readdirSync(out).length) throw new LocalCaError(`--out ${out} is not empty; refusing to overwrite`);
}

/** Writes ca.crt, server.crt and server.key into opts.out; returns the sha256 hex of the ca.crt bytes. */
export function generateLocalCa(opts: LocalCaOptions): string {
  const out = resolve(opts.out);
  prepareOut(out);
  const work = mkdtempSync(join(tmpdir(), "selfhost-local-ca-"));
  chmodSync(work, 0o700);
  const openssl = (...args: string[]) => {
    try { execFileSync("openssl", args, { cwd: work, stdio: ["ignore", "ignore", "pipe"] }); } catch (error) {
      const stderr = String((error as { stderr?: Buffer }).stderr ?? "").trim().split("\n").slice(-3).join("; ");
      throw new LocalCaError(`openssl ${args[0]} failed${stderr ? `: ${stderr}` : ""}`);
    }
  };
  try {
    const days = String(opts.days);
    // Self-sign through `x509 -req -extfile` rather than `req -x509 -addext`, which older LibreSSL lacks.
    writeFileSync(join(work, "ca.ext"), "basicConstraints=critical,CA:TRUE,pathlen:0\nkeyUsage=critical,keyCertSign,cRLSign\nsubjectKeyIdentifier=hash\n");
    openssl("req", "-new", "-newkey", "rsa:3072", "-nodes", "-subj", "/CN=96bulls-local-postgres-ca", "-keyout", "ca.key", "-out", "ca.csr");
    openssl("x509", "-req", "-in", "ca.csr", "-signkey", "ca.key", "-days", days, "-sha256", "-extfile", "ca.ext", "-out", "ca.crt");
    const san = opts.hosts.map(h => (isIP(h) ? `IP:${h}` : `DNS:${h}`)).join(",");
    writeFileSync(join(work, "server.ext"), `subjectAltName=${san}\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n`);
    openssl("req", "-new", "-newkey", "rsa:3072", "-nodes", "-subj", `/CN=${opts.hosts[0]}`, "-keyout", "server.key", "-out", "server.csr");
    openssl("x509", "-req", "-in", "server.csr", "-CA", "ca.crt", "-CAkey", "ca.key", "-CAcreateserial", "-days", days, "-sha256", "-extfile", "server.ext", "-out", "server.crt");
    const ca = readFileSync(join(work, "ca.crt"));
    const written: string[] = [];
    try {
      for (const [name, mode] of [["ca.crt", 0o644], ["server.crt", 0o644], ["server.key", 0o600]] as const) {
        const target = join(out, name);
        // "wx": never follow or replace something that appeared in --out meanwhile.
        writeFileSync(target, readFileSync(join(work, name)), { flag: "wx", mode });
        written.push(target);
        chmodSync(target, mode); // independent of the caller's umask
      }
    } catch (error) {
      for (const target of written) rmSync(target, { force: true });
      throw error;
    }
    return createHash("sha256").update(ca).digest("hex");
  } finally {
    rmSync(work, { recursive: true, force: true });
  }
}

if (import.meta.main) {
  try {
    const sha = generateLocalCa(parseLocalCaArgs(process.argv.slice(2)));
    process.stdout.write(`SESSION_POSTGRES_CA_SHA256=${sha}\n`);
  } catch (error) {
    process.stderr.write(`selfhost-local-ca: ${error instanceof LocalCaError ? error.message : String(error)}\n`);
    process.exit(2);
  }
}
