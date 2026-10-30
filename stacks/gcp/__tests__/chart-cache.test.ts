import { createHash } from "node:crypto";
import { execFileSync, spawn } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, relative } from "node:path";
import { describe, expect, test } from "bun:test";
import { prepareRelaycastChart } from "../chart-cache.ts";

function fixture(root: string) {
  const source = join(root, "source");
  const chart = join(source, "relaycast");
  mkdirSync(join(chart, "templates"), { recursive: true });
  writeFileSync(join(chart, "Chart.yaml"), "apiVersion: v2\nname: relaycast\nversion: 0.1.0\n");
  writeFileSync(join(chart, "templates", "service.yaml"), "kind: Service\n");
  const artifact = join(root, "relaycast-0.1.0.tgz");
  execFileSync("tar", ["-czf", artifact, "-C", source, "relaycast"]);
  const bytes = readFileSync(artifact);
  const digest = createHash("sha256").update(bytes).digest("hex");
  return { artifact, bytes, digest, cacheRoot: join(root, "cache") };
}

function treeDigest(root: string): string {
  const hash = createHash("sha256");
  const visit = (directory: string): void => {
    for (const entry of readdirSync(directory, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
      const path = join(directory, entry.name);
      const name = relative(root, path).replaceAll("\\", "/");
      if (entry.isDirectory()) {
        hash.update(`d\0${name}\0`);
        visit(path);
      } else if (entry.isFile()) {
        hash.update(`f\0${name}\0`);
        hash.update(readFileSync(path));
        hash.update("\0");
      }
    }
  };
  visit(root);
  return hash.digest("hex");
}

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
  test("populates once for concurrent consumers and rejects cache plus sidecar tampering", async () => {
    const root = mkdtempSync(join(tmpdir(), "96bulls-chart-cache-test-"));
    try {
      const { artifact, bytes, digest, cacheRoot } = fixture(root);
      const moduleUrl = new URL("../chart-cache.ts", import.meta.url).href;

      const [first, second] = await Promise.all([
        child(moduleUrl, artifact, digest, cacheRoot),
        child(moduleUrl, artifact, digest, cacheRoot),
      ]);
      expect(first).toEqual(second);
      expect(first.chart).toContain("name: relaycast");

      const prepared = prepareRelaycastChart({ bytes, digest, sourcePath: artifact }, { cacheRoot });
      writeFileSync(join(prepared, "Chart.yaml"), "tampered\n");
      const recordPath = join(prepared, "..", ".96bulls-verified-chart.json");
      const record = JSON.parse(readFileSync(recordPath, "utf8"));
      record.chartDigest = treeDigest(prepared);
      writeFileSync(recordPath, JSON.stringify(record));
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

  test("recovers a dead extractor lock but never steals a live one", () => {
    const root = mkdtempSync(join(tmpdir(), "96bulls-chart-lock-test-"));
    try {
      const dead = fixture(join(root, "dead"));
      const deadLock = join(dead.cacheRoot, `.${dead.digest}.lock`);
      mkdirSync(deadLock, { recursive: true });
      writeFileSync(
        join(deadLock, "owner.json"),
        JSON.stringify({ id: "dead-owner-00000000", pid: 2_147_483_647, createdAtMs: 1 }),
      );
      expect(
        prepareRelaycastChart(
          { bytes: dead.bytes, digest: dead.digest, sourcePath: dead.artifact },
          { cacheRoot: dead.cacheRoot, lockWaitMs: 20 },
        ),
      ).toContain(join(dead.digest, "relaycast"));

      const live = fixture(join(root, "live"));
      const liveLock = join(live.cacheRoot, `.${live.digest}.lock`);
      mkdirSync(liveLock, { recursive: true });
      writeFileSync(
        join(liveLock, "owner.json"),
        JSON.stringify({ id: "live-owner-00000000", pid: process.pid, createdAtMs: Date.now() }),
      );
      expect(() =>
        prepareRelaycastChart(
          { bytes: live.bytes, digest: live.digest, sourcePath: live.artifact },
          { cacheRoot: live.cacheRoot, lockWaitMs: 10, staleLockMs: 1 },
        ),
      ).toThrow(/cache is unavailable/);
      expect(readFileSync(join(liveLock, "owner.json"), "utf8")).toContain("live-owner");
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});
