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
import type {
  CapturedRelayauthChartPackage,
  CapturedRelaycastChartPackage,
} from "./config.ts";

interface ChartCacheRecord {
  packageDigest: string;
  chartDigest: string;
}

export interface RelaycastChartCacheOptions {
  cacheRoot?: string;
  lockWaitMs?: number;
  staleLockMs?: number;
}

export type RelayauthChartCacheOptions = RelaycastChartCacheOptions;

const CACHE_RECORD = ".96bulls-verified-chart.json";
const LOCK_OWNER = "owner.json";
const DEFAULT_LOCK_WAIT_MS = 15_000;
const DEFAULT_STALE_LOCK_MS = 30_000;

interface LockOwner {
  id: string;
  pid: number;
  createdAtMs: number;
}

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

function verifiedChartPath(
  cache: string,
  chartName: string,
  expectedPackageDigest: string,
  expectedChartDigest: string,
): string | undefined {
  const chart = join(cache, chartName);
  try {
    if (!statSync(chart).isDirectory()) return;
    const record = JSON.parse(readFileSync(join(cache, CACHE_RECORD), "utf8")) as ChartCacheRecord;
    if (
      record.packageDigest !== expectedPackageDigest ||
      record.chartDigest !== expectedChartDigest ||
      chartTreeDigest(chart) !== expectedChartDigest
    ) return;
    return chart;
  } catch {
    return;
  }
}

function readLockOwner(lock: string): LockOwner | undefined {
  try {
    const owner = JSON.parse(readFileSync(join(lock, LOCK_OWNER), "utf8")) as Partial<LockOwner>;
    if (
      typeof owner.id !== "string" || owner.id.length < 16 ||
      !Number.isInteger(owner.pid) || owner.pid! < 1 ||
      !Number.isFinite(owner.createdAtMs)
    ) return;
    return owner as LockOwner;
  } catch {
    return;
  }
}

function processIsAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return (error as NodeJS.ErrnoException).code !== "ESRCH";
  }
}

function recoverStaleLock(
  lock: string,
  cacheRoot: string,
  staleLockMs: number,
): boolean {
  let ageMs: number;
  try {
    ageMs = Date.now() - statSync(lock).mtimeMs;
  } catch {
    return true;
  }
  const observed = readLockOwner(lock);
  if (observed ? processIsAlive(observed.pid) : ageMs < staleLockMs) return false;

  const quarantine = join(cacheRoot, `.stale-lock.${process.pid}.${randomUUID()}`);
  try {
    renameSync(lock, quarantine);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return true;
    return false;
  }
  const moved = readLockOwner(quarantine);
  const sameOwner = observed
    ? moved?.id === observed.id && moved.pid === observed.pid
    : moved === undefined;
  if (!sameOwner) {
    try {
      renameSync(quarantine, lock);
    } catch {
      // Do not delete a lock whose owner changed while it was being inspected.
    }
    return false;
  }
  rmSync(quarantine, { recursive: true, force: true });
  return true;
}

function releaseOwnedLock(lock: string, owner: LockOwner, cacheRoot: string): void {
  if (readLockOwner(lock)?.id !== owner.id) return;
  const released = join(cacheRoot, `.released-lock.${process.pid}.${owner.id}`);
  try {
    renameSync(lock, released);
  } catch {
    return;
  }
  if (readLockOwner(released)?.id === owner.id)
    rmSync(released, { recursive: true, force: true });
}

/**
 * Materialize only the already captured package. A digest cache is populated
 * once under an exclusive directory lock and never replaced while a consumer
 * may be rendering it. Existing cache bytes are validated before reuse.
 */
function prepareVerifiedChart(
  captured: CapturedRelaycastChartPackage,
  chartName: "relaycast" | "relayauth",
  version: string,
  label: "Relaycast" | "RelayAuth",
  options: RelaycastChartCacheOptions = {},
): string {
  if (createHash("sha256").update(captured.bytes).digest("hex") !== captured.digest)
    throw new Error(`96bulls: captured ${label} package changed after verification. Nothing was created.`);

  const cacheRoot = options.cacheRoot ?? join(tmpdir(), `96bulls-${chartName}-charts-v3`);
  const extracted = join(cacheRoot, captured.digest);
  const lock = join(cacheRoot, `.${captured.digest}.lock`);
  const deadline = Date.now() + (options.lockWaitMs ?? DEFAULT_LOCK_WAIT_MS);
  const staleLockMs = options.staleLockMs ?? DEFAULT_STALE_LOCK_MS;
  if (!Number.isFinite(staleLockMs) || staleLockMs < 1)
    throw new Error("96bulls: verified Relaycast chart cache lock policy is invalid. Nothing was created.");
  mkdirSync(cacheRoot, { recursive: true, mode: 0o700 });

  // The expected tree hash comes from a fresh extraction of the already
  // captured, digest-verified archive. The mutable cache record is evidence,
  // never the trust root for accepting cached chart bytes.
  const staging = join(cacheRoot, `.${captured.digest}.${process.pid}.${randomUUID()}`);
  mkdirSync(staging, { mode: 0o700 });
  try {
    const artifact = join(staging, `${chartName}-${version}.tgz`);
    writeFileSync(artifact, captured.bytes, { mode: 0o600 });
    execFileSync("tar", ["-xzf", artifact, "-C", staging], { stdio: "ignore" });
    rmSync(artifact, { force: true });
    if (!statSync(join(staging, chartName)).isDirectory()) throw new Error(`missing ${chartName} chart`);
  } catch {
    rmSync(staging, { recursive: true, force: true });
    throw new Error(`96bulls: verified ${label} package could not be cached. Nothing was created.`);
  }
  const trustedChartDigest = chartTreeDigest(join(staging, chartName));
  let owner: LockOwner | undefined;

  for (;;) {
    const ready = verifiedChartPath(extracted, chartName, captured.digest, trustedChartDigest);
    if (ready) {
      rmSync(staging, { recursive: true, force: true });
      return ready;
    }
    try {
      mkdirSync(lock, { mode: 0o700 });
      owner = { id: randomUUID(), pid: process.pid, createdAtMs: Date.now() };
      writeFileSync(join(lock, LOCK_OWNER), JSON.stringify(owner), { encoding: "utf8", mode: 0o600 });
      break;
    } catch (error) {
      if (
        (error as NodeJS.ErrnoException).code !== "EEXIST" ||
        (!recoverStaleLock(lock, cacheRoot, staleLockMs) && Date.now() >= deadline)
      ) {
        rmSync(staging, { recursive: true, force: true });
        throw new Error(`96bulls: verified ${label} chart cache is unavailable. Nothing was created.`);
      }
      pause(Math.min(50, Math.max(1, deadline - Date.now())));
    }
  }

  try {
    const ready = verifiedChartPath(extracted, chartName, captured.digest, trustedChartDigest);
    if (ready) {
      rmSync(staging, { recursive: true, force: true });
      return ready;
    }
    try {
      statSync(extracted);
      throw new Error("invalid existing cache");
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }

    const chart = join(staging, chartName);
    writeFileSync(
      join(staging, CACHE_RECORD),
      JSON.stringify({ packageDigest: captured.digest, chartDigest: trustedChartDigest } satisfies ChartCacheRecord),
      { encoding: "utf8", mode: 0o600 },
    );
    renameSync(staging, extracted);
    const prepared = verifiedChartPath(extracted, chartName, captured.digest, trustedChartDigest);
    if (!prepared) throw new Error("prepared cache did not validate");
    return prepared;
  } catch {
    rmSync(staging, { recursive: true, force: true });
    throw new Error(`96bulls: verified ${label} package could not be cached. Nothing was created.`);
  } finally {
    if (owner) releaseOwnedLock(lock, owner, cacheRoot);
  }
}

export function prepareRelaycastChart(
  captured: CapturedRelaycastChartPackage,
  options: RelaycastChartCacheOptions = {},
): string {
  return prepareVerifiedChart(captured, "relaycast", "0.1.0", "Relaycast", options);
}

export function prepareRelayauthChart(
  captured: CapturedRelayauthChartPackage,
  options: RelayauthChartCacheOptions = {},
): string {
  return prepareVerifiedChart(captured, "relayauth", "0.1.1", "RelayAuth", options);
}
