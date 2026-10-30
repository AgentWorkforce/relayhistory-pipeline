import { createHash, randomUUID } from "node:crypto";
import {
  mkdirSync,
  readFileSync,
  readdirSync,
  renameSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join, relative } from "node:path";
import { execFileSync } from "node:child_process";
import type { CapturedRelaycastChartPackage } from "./config.ts";

interface ChartCacheRecord {
  packageDigest: string;
  chartDigest: string;
}

export interface RelaycastChartCacheOptions {
  cacheRoot?: string;
  lockWaitMs?: number;
}

const CACHE_RECORD = ".96bulls-verified-chart.json";
const DEFAULT_LOCK_WAIT_MS = 15_000;

function pause(milliseconds: number): void {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, milliseconds);
}

function chartTreeDigest(root: string): string {
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
      } else {
        throw new Error("unsupported chart entry");
      }
    }
  };
  visit(root);
  return hash.digest("hex");
}

function verifiedChartPath(cache: string, expectedDigest: string): string | undefined {
  const chart = join(cache, "relaycast");
  try {
    if (!statSync(chart).isDirectory()) return;
    const record = JSON.parse(readFileSync(join(cache, CACHE_RECORD), "utf8")) as ChartCacheRecord;
    if (record.packageDigest !== expectedDigest || record.chartDigest !== chartTreeDigest(chart)) return;
    return chart;
  } catch {
    return;
  }
}

/**
 * Materialize only the already captured package. A digest cache is populated
 * once under an exclusive directory lock and never replaced while a consumer
 * may be rendering it. Existing cache bytes are validated before reuse.
 */
export function prepareRelaycastChart(
  captured: CapturedRelaycastChartPackage,
  options: RelaycastChartCacheOptions = {},
): string {
  if (createHash("sha256").update(captured.bytes).digest("hex") !== captured.digest)
    throw new Error("96bulls: captured Relaycast package changed after verification. Nothing was created.");

  const cacheRoot = options.cacheRoot ?? join(tmpdir(), "96bulls-relaycast-charts-v2");
  const extracted = join(cacheRoot, captured.digest);
  const lock = join(cacheRoot, `.${captured.digest}.lock`);
  const deadline = Date.now() + (options.lockWaitMs ?? DEFAULT_LOCK_WAIT_MS);
  mkdirSync(cacheRoot, { recursive: true, mode: 0o700 });

  for (;;) {
    const ready = verifiedChartPath(extracted, captured.digest);
    if (ready) return ready;
    try {
      mkdirSync(lock, { mode: 0o700 });
      break;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST" || Date.now() >= deadline)
        throw new Error("96bulls: verified Relaycast chart cache is unavailable. Nothing was created.");
      pause(Math.min(50, Math.max(1, deadline - Date.now())));
    }
  }

  const staging = join(cacheRoot, `.${captured.digest}.${process.pid}.${randomUUID()}`);
  try {
    const ready = verifiedChartPath(extracted, captured.digest);
    if (ready) return ready;
    try {
      statSync(extracted);
      throw new Error("invalid existing cache");
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }

    mkdirSync(staging, { mode: 0o700 });
    const artifact = join(staging, "relaycast-0.1.0.tgz");
    writeFileSync(artifact, captured.bytes, { mode: 0o600 });
    execFileSync("tar", ["-xzf", artifact, "-C", staging], { stdio: "ignore" });
    rmSync(artifact, { force: true });
    const chart = join(staging, "relaycast");
    if (!statSync(chart).isDirectory()) throw new Error("published package did not contain relaycast/");
    writeFileSync(
      join(staging, CACHE_RECORD),
      JSON.stringify({ packageDigest: captured.digest, chartDigest: chartTreeDigest(chart) } satisfies ChartCacheRecord),
      { encoding: "utf8", mode: 0o600 },
    );
    renameSync(staging, extracted);
    const prepared = verifiedChartPath(extracted, captured.digest);
    if (!prepared) throw new Error("prepared cache did not validate");
    return prepared;
  } catch {
    rmSync(staging, { recursive: true, force: true });
    throw new Error("96bulls: verified Relaycast package could not be cached. Nothing was created.");
  } finally {
    rmSync(lock, { recursive: true, force: true });
  }
}
