import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { connect, createServer } from "node:tls";
import { postgresConnectionString, postgresTlsOptions } from "../postgres-tls.ts";
import { SUPABASE_PROD_CA_2021 } from "../../session-pipeline/supabase-ca.ts";

let dir: string;
const hash = (bytes: Uint8Array) => createHash("sha256").update(bytes).digest("hex");
const env = (file = "ca.crt") => ({ SESSION_POSTGRES_TLS_PROFILE: "custom-ca", SESSION_POSTGRES_CA_FILE: join(dir, file), SESSION_POSTGRES_CA_SHA256: hash(readFileSync(join(dir, file))) });
beforeAll(() => {
  const cache = join(import.meta.dir, "../../../node_modules/.cache");
  mkdirSync(cache, { recursive: true });
  dir = mkdtempSync(join(cache, "postgres-tls-"));
  const run = (...args: string[]) => execFileSync("openssl", args, { cwd: dir, stdio: "ignore" });
  for (const name of ["ca", "other"]) run("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", `${name}.key`, "-out", `${name}.crt`, "-days", "1", "-subj", `/CN=synthetic-${name}`, "-addext", "basicConstraints=critical,CA:TRUE");
  run("req", "-newkey", "rsa:2048", "-nodes", "-keyout", "server.key", "-out", "server.csr", "-subj", "/CN=localhost");
  writeFileSync(join(dir, "extensions"), "subjectAltName=DNS:localhost\nbasicConstraints=CA:FALSE\nextendedKeyUsage=serverAuth\n");
  run("x509", "-req", "-in", "server.csr", "-CA", "ca.crt", "-CAkey", "ca.key", "-CAcreateserial", "-out", "server.crt", "-days", "1", "-extfile", "extensions");
});
afterAll(() => { if (dir) rmSync(dir, { recursive: true, force: true }); });

describe("PostgreSQL TLS", () => {
  test("preserves pinned Supabase default and refuses ignored/unknown trust inputs", () => {
    expect(postgresTlsOptions({})).toEqual({ ca: SUPABASE_PROD_CA_2021, rejectUnauthorized: true });
    for (const input of [{ SESSION_POSTGRES_TLS_PROFILE: "" }, { SESSION_POSTGRES_TLS_PROFILE: "system" }, { SESSION_POSTGRES_CA_FILE: "unexpected" }, { ...env(), SESSION_POSTGRES_CA_SHA256: undefined }, { ...env(), SESSION_POSTGRES_CA_SHA256: "0".repeat(64) }, { ...env(), SESSION_POSTGRES_CA_FILE: join(dir, "absent") }]) expect(() => postgresTlsOptions(input)).toThrow("TLS configuration is invalid");
  });
  test("CA files require exact approved bytes and certificates only", () => {
    writeFileSync(join(dir, "invalid"), "not a CA");
    expect(() => postgresTlsOptions(env("invalid"))).toThrow();
    expect(() => postgresTlsOptions(env("server.crt"))).toThrow();
    writeFileSync(join(dir, "mixed"), readFileSync(join(dir, "ca.crt"), "utf8") + readFileSync(join(dir, "ca.key"), "utf8"));
    expect(() => postgresTlsOptions(env("mixed"))).toThrow();
  });
  test("URL TLS/options cannot weaken verification or change the approved host", () => {
    const url = new URL(postgresConnectionString("postgresql://u:p@localhost/db?sslmode=disable&SSLROOTCERT=bad&sslkey=bad&options=-c%20ssl%3Doff&application_name=fixture"));
    expect([...url.searchParams.keys()]).toEqual(["application_name"]);
    expect(url.hostname).toBe("localhost");
    for (const key of ["host", "hostaddr", "port", "user", "password", "database"]) expect(() => postgresConnectionString(`postgresql://u:p@localhost/db?${key}=other`)).toThrow();
    expect(() => postgresConnectionString("http://localhost/db")).toThrow();
  });
  test("real TLS handshake accepts the approved CA+hostname and refuses another CA/hostname", async () => {
    const server = createServer({ key: readFileSync(join(dir, "server.key")), cert: readFileSync(join(dir, "server.crt")) }, socket => socket.end());
    server.on("tlsClientError", () => {});
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as { port: number }).port;
    const handshake = (caFile: string, servername: string) => new Promise<boolean>(resolve => {
      const socket = connect({ ...postgresTlsOptions(env(caFile)), host: "127.0.0.1", port, servername }, () => { const ok = socket.authorized; socket.destroy(); resolve(ok); });
      socket.setTimeout(2000, () => { socket.destroy(); resolve(false); });
      socket.once("error", () => { socket.destroy(); resolve(false); });
    });
    try {
      expect(await handshake("ca.crt", "localhost")).toBe(true);
      expect(await handshake("other.crt", "localhost")).toBe(false);
      expect(await handshake("ca.crt", "wrong.invalid")).toBe(false);
    } finally { await new Promise<void>(resolve => server.close(() => resolve())); }
  }, 10_000);
});
