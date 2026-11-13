import { mkdir, open, readFile, rm, lstat, rename } from "node:fs/promises";
import { resolve } from "node:path";
import type { Dataset } from "./contracts.ts";

export async function readJson(path: string, max = 16_000_000): Promise<unknown> {
  const file = await open(path, "r");
  try {
    const info = await file.stat();
    if (!info.isFile() || info.size > max) throw new Error("invalid_or_oversized_file");
    return JSON.parse(await file.readFile("utf8"));
  } finally { await file.close(); }
}
export function requireUnexpired(dataset: Dataset, now = new Date()) {
  if (dataset.provenance.kind === "production" && Date.parse(dataset.provenance.expiresAt) <= now.getTime()) throw new Error("production_dataset_expired");
}
/** New directory only. A crash leaves an expiring manifest; normal failures erase partial copies. */
export async function writeRun(path: string, files: Record<string, unknown>, metadata: { expiresAt: string; sessionKeys: string[] }) {
  for (const name of Object.keys(files)) if (!/^[a-z-]+\.(json|md)$/.test(name) || name === "manifest.json") throw new Error("invalid_artifact_name");
  await mkdir(path, { mode: 0o700 });
  const manifest = { format: "overlap-alert-run/1", ...metadata, files: Object.keys(files) };
  async function write(name: string, value: unknown) {
    const f = await open(resolve(path, name), "wx", 0o600);
    try { await f.writeFile(typeof value === "string" ? value : JSON.stringify(value, null, 2) + "\n"); }
    finally { await f.close(); }
  }
  try {
    await write("manifest.json", { ...manifest, complete: false });
    for (const [name, value] of Object.entries(files)) await write(name, value);
    await write("manifest-next.json", { ...manifest, complete: true });
    await rename(resolve(path, "manifest-next.json"), resolve(path, "manifest.json"));
  } catch (error) { await rm(path, { recursive: true, force: true }); throw error; }
}
/** Explicit run erasure. Erase the entire derived run when any included session is erased. */
export async function eraseRun(path: string) {
  const stat = await lstat(path);
  if (!stat.isDirectory() || stat.isSymbolicLink()) throw new Error("not_an_alert_run");
  const v = JSON.parse(await readFile(resolve(path, "manifest.json"), "utf8"));
  if (v?.format !== "overlap-alert-run/1") throw new Error("not_an_alert_run");
  await rm(path, { recursive: true });
}

/** TTL cleanup for a private artifact root; skips unrelated directories and symlinks. */
export async function purgeExpiredRuns(root: string, now = new Date()): Promise<number> {
  const { readdir } = await import("node:fs/promises");
  let erased = 0;
  for (const entry of await readdir(root, { withFileTypes: true })) {
    if (!entry.isDirectory() || entry.isSymbolicLink()) continue;
    const path = resolve(root, entry.name);
    let manifest: unknown;
    try { manifest = await readJson(resolve(path, "manifest.json"), 256_000); } catch { continue; }
    if (!manifest || typeof manifest !== "object") continue;
    const m = manifest as Record<string, unknown>;
    if (m.format !== "overlap-alert-run/1" || typeof m.expiresAt !== "string" || !Number.isFinite(Date.parse(m.expiresAt))) continue;
    if (Date.parse(m.expiresAt) <= now.getTime()) { await eraseRun(path); erased++; }
  }
  return erased;
}
