import { expect, test } from "bun:test";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { collectorTransport } from "../../shared/analytics-export.ts";
import { briefCollectorFetch } from "../analytics-transport.ts";

test("brief collector trust is explicit and does not bypass hostname verification", async () => {
  const directory = mkdtempSync(join(import.meta.dir, ".brief-analytics-tls-"));
  const run = (...args: string[]) => execFileSync("openssl", args, { cwd: directory, stdio: "ignore" });
  try {
    run("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", "ca.key", "-out", "ca.crt",
      "-days", "1", "-subj", "/CN=brief-analytics-test", "-addext", "basicConstraints=critical,CA:TRUE");
    run("req", "-newkey", "rsa:2048", "-nodes", "-keyout", "server.key", "-out", "server.csr", "-subj", "/CN=localhost");
    writeFileSync(join(directory, "extensions"), "subjectAltName=DNS:localhost\nbasicConstraints=CA:FALSE\nextendedKeyUsage=serverAuth\n");
    run("x509", "-req", "-in", "server.csr", "-CA", "ca.crt", "-CAkey", "ca.key", "-CAcreateserial",
      "-out", "server.crt", "-days", "1", "-extfile", "extensions");
    const caFile = join(directory, "ca.crt");
    let received = 0;
    const server = Bun.serve({
      port: 0, hostname: "127.0.0.1",
      tls: { key: readFileSync(join(directory, "server.key")), cert: readFileSync(join(directory, "server.crt")) },
      fetch() {
        received++;
        return new Response(null, { status: 204 });
      },
    });
    try {
      const url = `https://localhost:${server.port}/v1/events`;
      const config = { sink: "collector", url, token: "synthetic-brief-producer", deploymentId: "test" } as const;
      const signal = AbortSignal.timeout(2_000);
      const direct = collectorTransport(config);
      expect(await direct.send([], signal)).toEqual({ kind: "retry" });
      const fetch = briefCollectorFetch(config, caFile)!;
      const custom = collectorTransport({ ...config, fetch });
      expect(await custom.send([], signal)).toEqual({ kind: "accepted" });
      const wrongHost = { ...config, url: url.replace("localhost", "127.0.0.1") };
      expect(await collectorTransport({ ...wrongHost, fetch: briefCollectorFetch(wrongHost, caFile)! }).send([], signal)).toEqual({ kind: "retry" });
      expect(received).toBe(1);
      expect(() => briefCollectorFetch(config, join(directory, "server.crt"))).toThrow("public CA bundle");
      expect(() => briefCollectorFetch({ sink: "noop" }, caFile)).toThrow("requires an HTTPS collector");
    } finally {
      await server.stop(true);
    }
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
