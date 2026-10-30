import { createHash } from "node:crypto";
import { execFileSync, spawn } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, test } from "bun:test";
import { prepareRelaycastChart } from "../chart-cache.ts";

function child(
  moduleUrl: string,
  artifact: string,
  digest: string,
  cacheRoot: string,
): Promise<{ path: string; chart: string }> {
  const source = `
    const { readFileSync } = await import("node:fs");
    const { prepareRelaycastChart } = await import(process.argv[1]);
    const bytes = readFileSync(process.argv[2]);
    const path = prepareRelaycastChart(
      { bytes, digest: process.argv[3], sourcePath: process.argv[2] },
      { cacheRoot: process.argv[4] },
    );
    process.stdout.write(JSON.stringify({ path, chart: readFileSync(path + "/Chart.yaml", "utf8") }));
  `;
  return new Promise((resolve, reject) => {
    const process = spawn(globalThis.process.execPath, ["-e", source, moduleUrl, artifact, digest, cacheRoot], {
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    process.stdout.on("data", (chunk) => (stdout += chunk));
    process.stderr.on("data", (chunk) => (stderr += chunk));
    process.once("error", reject);
    process.once("exit", (code) => {
      if (code !== 0) reject(new Error(stderr));
      else resolve(JSON.parse(stdout));
    });
  });
}

describe("verified Relaycast chart cache", () => {
  test("populates once for concurrent consumers and validates before reuse", async () => {
    const root = mkdtempSync(join(tmpdir(), "96bulls-chart-cache-test-"));
    try {
      const source = join(root, "source");
      const chart = join(source, "relaycast");
      mkdirSync(join(chart, "templates"), { recursive: true });
      writeFileSync(join(chart, "Chart.yaml"), "apiVersion: v2\nname: relaycast\nversion: 0.1.0\n");
      writeFileSync(join(chart, "templates", "service.yaml"), "kind: Service\n");
      const artifact = join(root, "relaycast-0.1.0.tgz");
      execFileSync("tar", ["-czf", artifact, "-C", source, "relaycast"]);
      const bytes = readFileSync(artifact);
      const digest = createHash("sha256").update(bytes).digest("hex");
      const cacheRoot = join(root, "cache");
      const moduleUrl = new URL("../chart-cache.ts", import.meta.url).href;

      const [first, second] = await Promise.all([
        child(moduleUrl, artifact, digest, cacheRoot),
        child(moduleUrl, artifact, digest, cacheRoot),
      ]);
      expect(first).toEqual(second);
      expect(first.chart).toContain("name: relaycast");

      const prepared = prepareRelaycastChart({ bytes, digest, sourcePath: artifact }, { cacheRoot });
      writeFileSync(join(prepared, "Chart.yaml"), "tampered\n");
      expect(() => prepareRelaycastChart({ bytes, digest, sourcePath: artifact }, { cacheRoot })).toThrow(
        /could not be cached/,
      );
      expect(readFileSync(join(prepared, "Chart.yaml"), "utf8")).toBe("tampered\n");
      expect(readFileSync(new URL("../chart-cache.ts", import.meta.url), "utf8")).not.toContain(
        "rmSync(extracted",
      );
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});
