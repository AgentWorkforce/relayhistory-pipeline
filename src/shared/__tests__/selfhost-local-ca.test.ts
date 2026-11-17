import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { createHash, X509Certificate } from "node:crypto";
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { connect, createServer } from "node:tls";
import { postgresTlsOptions } from "../postgres-tls.ts";
import { DEFAULT_HOSTS, parseLocalCaArgs } from "../../../scripts/selfhost-local-ca.ts";

const script = join(import.meta.dir, "../../../scripts/selfhost-local-ca.ts");
let dir: string;
let out: string;
let printed: string;
const run = (...args: string[]) => spawnSync(process.execPath, [script, ...args], { encoding: "utf8" });

beforeAll(() => {
  const cache = join(import.meta.dir, "../../../node_modules/.cache");
  mkdirSync(cache, { recursive: true });
  dir = mkdtempSync(join(cache, "selfhost-local-ca-"));
  out = join(dir, "pg-tls");
  const result = run("--out", out);
  expect(result.status).toBe(0);
  expect(result.stderr).toBe("");
  printed = result.stdout;
});
afterAll(() => { if (dir) rmSync(dir, { recursive: true, force: true }); });

describe("scripts/selfhost-local-ca.ts", () => {
  test("prints exactly one SESSION_POSTGRES_CA_SHA256 line and no key material", () => {
    expect(printed).toMatch(/^SESSION_POSTGRES_CA_SHA256=[a-f0-9]{64}\n$/);
    expect(printed).not.toContain("PRIVATE KEY");
    expect(readdirSync(out).sort()).toEqual(["ca.crt", "server.crt", "server.key"]);
    expect(statSync(join(out, "server.key")).mode & 0o777).toBe(0o600);
    expect(statSync(join(out, "ca.crt")).mode & 0o777).toBe(0o644);
    expect(readFileSync(join(out, "server.key"), "utf8")).toContain("PRIVATE KEY");
  });

  test("the printed sha is the pin postgres-tls.ts accepts for ca.crt", () => {
    const sha = printed.trim().split("=")[1];
    expect(sha).toBe(createHash("sha256").update(readFileSync(join(out, "ca.crt"))).digest("hex"));
    const options = postgresTlsOptions({ SESSION_POSTGRES_TLS_PROFILE: "custom-ca", SESSION_POSTGRES_CA_FILE: join(out, "ca.crt"), SESSION_POSTGRES_CA_SHA256: sha });
    expect(options.rejectUnauthorized).toBe(true);
    expect(options.ca).toBe(readFileSync(join(out, "ca.crt"), "utf8"));
  });

  test("server.crt chains to ca.crt and carries the default SANs", () => {
    const ca = new X509Certificate(readFileSync(join(out, "ca.crt")));
    const server = new X509Certificate(readFileSync(join(out, "server.crt")));
    expect(ca.ca).toBe(true);
    expect(server.ca).toBe(false);
    expect(server.checkIssued(ca)).toBe(true);
    expect(server.verify(ca.publicKey)).toBe(true);
    expect(server.subjectAltName).toBe("DNS:db, DNS:localhost, IP Address:127.0.0.1");
    for (const host of ["db", "localhost"]) expect(server.checkHost(host)).toBe(host);
    expect(server.checkIP("127.0.0.1")).toBe("127.0.0.1");
    const days = (Date.parse(server.validTo) - Date.parse(server.validFrom)) / 86_400_000;
    expect(Math.round(days)).toBe(825);
  });

  test("a TLS client using postgresTlsOptions completes a verified handshake to localhost", async () => {
    const sha = printed.trim().split("=")[1];
    const options = postgresTlsOptions({ SESSION_POSTGRES_TLS_PROFILE: "custom-ca", SESSION_POSTGRES_CA_FILE: join(out, "ca.crt"), SESSION_POSTGRES_CA_SHA256: sha });
    const server = createServer({ cert: readFileSync(join(out, "server.crt")), key: readFileSync(join(out, "server.key")) }, socket => socket.end());
    await new Promise<void>(done => server.listen(0, "127.0.0.1", done));
    try {
      const port = (server.address() as { port: number }).port;
      const authorized = await new Promise<boolean>((done, fail) => {
        const socket = connect({ ...options, host: "127.0.0.1", port, servername: "localhost" }, () => { done(socket.authorized); socket.end(); });
        socket.on("error", fail);
      });
      expect(authorized).toBe(true);
    } finally { server.close(); }
  });

  test("refuses a non-empty or non-directory --out and leaves it untouched", () => {
    const busy = join(dir, "busy");
    mkdirSync(busy);
    writeFileSync(join(busy, "keep"), "x");
    const result = run("--out", busy);
    expect(result.status).toBe(2);
    expect(result.stdout).toBe("");
    expect(result.stderr).toContain("not empty");
    expect(readdirSync(busy)).toEqual(["keep"]);
    const file = join(dir, "file");
    writeFileSync(file, "x");
    expect(run("--out", file).status).toBe(2);
    expect(run("--out", out).status).toBe(2);
    expect(existsSync(join(out, "ca.srl"))).toBe(false);
  });

  test("custom hosts and days; invalid arguments are refused", () => {
    expect(parseLocalCaArgs(["--out", "x"])).toEqual({ out: "x", hosts: [...DEFAULT_HOSTS], days: 825 });
    expect(parseLocalCaArgs(["--out", "x", "--host", "pg.local", "--host", "::1", "--days", "30"])).toEqual({ out: "x", hosts: ["pg.local", "::1"], days: 30 });
    for (const argv of [[], ["--out"], ["--out", "x", "--days", "0"], ["--out", "x", "--days", "1e3"], ["--out", "x", "--host", "a b"], ["--out", "x", "--host", "DNS:evil,IP:1.2.3.4"], ["--out", "x", "--bogus"]]) {
      expect(() => parseLocalCaArgs(argv)).toThrow();
    }
    const custom = join(dir, "custom");
    const result = run("--out", custom, "--host", "pg.local", "--host", "::1", "--days", "30");
    expect(result.status).toBe(0);
    const server = new X509Certificate(readFileSync(join(custom, "server.crt")));
    expect(server.subjectAltName).toBe("DNS:pg.local, IP Address:0:0:0:0:0:0:0:1");
  });

  test("file modes do not depend on the caller's umask", () => {
    const strict = join(dir, "strict");
    const result = spawnSync("sh", ["-c", 'umask 077 && exec "$0" "$@"', process.execPath, script, "--out", strict], { encoding: "utf8" });
    expect(result.status).toBe(0);
    expect(statSync(join(strict, "ca.crt")).mode & 0o777).toBe(0o644);
    expect(statSync(join(strict, "server.crt")).mode & 0o777).toBe(0o644);
    expect(statSync(join(strict, "server.key")).mode & 0o777).toBe(0o600);
  });
});
