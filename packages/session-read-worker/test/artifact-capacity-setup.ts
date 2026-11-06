/** Node-only fixture server. Streams the real producer's synthetic objects from
 * disk; the workerd isolate never loads the full corpus or production config. */
import { createServer } from "node:http";
import { createReadStream, readFileSync, statSync, existsSync } from "node:fs";
import { resolve, sep } from "node:path";
import type { TestProject } from "vitest/node";
declare module "vitest" {
  export interface ProvidedContext {
    artifactFixtureUrl: string;
  }
}
export default async function setup(project: TestProject) {
  const fixture = process.env.CANONICAL_ARTIFACT_TEST_FIXTURE;
  if (!fixture)
    throw new Error(
      "CANONICAL_ARTIFACT_TEST_FIXTURE must point to synthetic Node producer fixture",
    );
  const root = resolve(fixture, "objects"),
    ack = JSON.parse(readFileSync(resolve(fixture, "claim.json"), "utf8"));
  if (ack.version !== 1 || ack.current !== true || !ack.identity || !ack.index)
    throw new Error("fixture claim must be an acknowledged v1 publication");
  const proof = JSON.parse(
    readFileSync(resolve(fixture, "proof.json"), "utf8"),
  );
  const namespaces = resolve(fixture, "namespaces.json");
  if (existsSync(namespaces))
    proof.namespaces = JSON.parse(readFileSync(namespaces, "utf8"));
  let requests = 0,
    totalBytes = 0,
    maxBytes = 0;
  const server = createServer((req, res) => {
    if (req.url === "/proof") {
      res.setHeader("content-type", "application/json");
      res.end(JSON.stringify(proof));
      return;
    }
    if (req.url === "/claim") {
      res.setHeader("content-type", "application/json");
      res.end(JSON.stringify(ack));
      return;
    }
    if (req.url === "/stats") {
      res.setHeader("content-type", "application/json");
      res.end(JSON.stringify({ requests, totalBytes, maxBytes }));
      return;
    }
    if (req.url === "/reset") {
      requests = totalBytes = maxBytes = 0;
      res.end("ok");
      return;
    }
    try {
      const url = new URL(req.url ?? "/", "http://fixture");
      if (url.pathname !== "/object") throw new Error("route");
      const key = url.searchParams.get("key");
      if (
        !key ||
        key.startsWith("/") ||
        key.includes("\\") ||
        key.split("/").some((p) => p === ".." || p === ".")
      )
        throw new Error("key");
      const file = resolve(root, key);
      if (!file.startsWith(root + sep)) throw new Error("prefix");
      const size = statSync(file).size;
      if (size > 256 * 1024) throw new Error("unbounded object");
      requests++;
      totalBytes += size;
      maxBytes = Math.max(maxBytes, size);
      res.setHeader("content-length", size);
      res.setHeader("content-type", "application/octet-stream");
      createReadStream(file).pipe(res);
    } catch {
      res.statusCode = 404;
      res.end("fixture object missing");
    }
  });
  await new Promise<void>((done) => server.listen(0, "127.0.0.1", done));
  const address = server.address();
  if (!address || typeof address === "string")
    throw new Error("fixture server");
  project.provide("artifactFixtureUrl", `http://127.0.0.1:${address.port}`);
  return () =>
    new Promise<void>((done, reject) =>
      server.close((error) => (error ? reject(error) : done())),
    );
}
