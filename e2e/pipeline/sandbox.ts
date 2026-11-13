/**
 * `--target sandbox`: the same assertions as `--target local`, on a fresh, isolated instance of the stack in a
 * throwaway Blaxel microVM (blaxel/docker-in-sandbox), never on the developer's machine.
 *
 *   1. snapshot the working tree as a commit (flows/lib/remote-ci.mjs snapshot: tracked and staged edits, nothing
 *      untracked, HEAD and the index untouched) and `git archive` it;
 *   2. create a sandbox named after the snapshot, upload the archive (multipart filesystem API), unpack it;
 *   3. run e2e/pipeline/sandbox-run.sh there (CI's bun, the locked install, run.ts --target local on the sandbox's
 *      own Docker), wait for it in bounded long-polls;
 *   4. copy its report.md and report.json back into this checkout's report dir, and delete the sandbox, always.
 *
 * Credentials: BL_API_KEY and BL_WORKSPACE from the environment only (for example `op run -- bun run
 * e2e:pipeline --target sandbox`); nothing is read from or written to disk. The stack's own credentials are
 * generated inside the sandbox, as locally, and never leave it.
 */
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = fileURLToPath(new URL("../..", import.meta.url));
const API = "https://api.blaxel.ai/v0";
const IMAGE = "blaxel/docker-in-sandbox:latest";
const MEMORY_MB = 16384;
const DOCKER_DISK_MB = 60_000;
/** The whole lane's ceiling inside the sandbox: the image build, the install and the steps. */
const RUN_MINUTES = 40;
const PART_BYTES = 5 * 1024 * 1024;

export interface SandboxOptions { steps?: string[]; reportDir: string }

function credentials() {
  const key = process.env.BL_API_KEY?.trim();
  const workspace = process.env.BL_WORKSPACE?.trim();
  if (!key || !workspace) throw new Error("--target sandbox needs BL_API_KEY and BL_WORKSPACE in the environment (from 1Password, e.g. `op run --`); nothing is read from disk");
  return { key, workspace };
}

async function api(path: string, init: RequestInit & { base?: string } = {}) {
  const { key, workspace } = credentials();
  const res = await fetch(`${init.base ?? API}${path}`, {
    ...init,
    headers: { authorization: `Bearer ${key}`, "x-blaxel-workspace": workspace, ...(init.body && !(init.body instanceof FormData) ? { "content-type": "application/json" } : {}), ...init.headers },
    signal: AbortSignal.timeout(150_000),
  });
  const text = await res.text();
  let json: any = null;
  try { json = JSON.parse(text); } catch {}
  // Never echo request headers; the body of an error answer is the API's own message.
  if (!res.ok) throw new Error(`Blaxel ${init.method ?? "GET"} ${path.split("?")[0]}: HTTP ${res.status} ${text.slice(0, 200)}`);
  return json ?? text;
}

/** The snapshot commit of the working tree and a gzipped archive of it. */
async function archive(dir: string): Promise<{ sha: string; file: string }> {
  const { snapshot } = await import("../../flows/lib/remote-ci.mjs" as string) as { snapshot: () => string };
  const sha = snapshot();
  const file = join(dir, "repo.tgz");
  execFileSync("git", ["-C", ROOT, "archive", "--format=tar.gz", "-o", file, sha]);
  return { sha, file };
}

async function upload(base: string, path: string, bytes: Buffer) {
  const enc = encodeURIComponent(path);
  const { uploadId } = await api(`/filesystem-multipart/initiate/${enc}`, { base, method: "POST", body: JSON.stringify({ permissions: "0644" }) });
  const parts: { partNumber: number; etag: string }[] = [];
  for (let i = 0, n = 1; i < bytes.length; i += PART_BYTES, n++) {
    const form = new FormData();
    form.append("file", new Blob([bytes.subarray(i, i + PART_BYTES)]), `part-${n}`);
    const r = await api(`/filesystem-multipart/${uploadId}/part?partNumber=${n}`, { base, method: "PUT", body: form });
    parts.push({ partNumber: n, etag: r.etag });
  }
  await api(`/filesystem-multipart/${uploadId}/complete`, { base, method: "POST", body: JSON.stringify({ parts }) });
}

async function exec(base: string, name: string, command: string, timeoutMinutes: number): Promise<{ exitCode: number; logs: string }> {
  await api("/process", { base, method: "POST", body: JSON.stringify({ name, command, workingDir: "/", waitForCompletion: false, keepAlive: true }) });
  const deadline = Date.now() + timeoutMinutes * 60_000;
  for (;;) {
    const p = await api(`/process/${name}/wait?timeout=110`, { base });
    if (p.status !== "running") return { exitCode: p.exitCode, logs: String(p.logs ?? "") };
    if (Date.now() > deadline) throw new Error(`${name} still running after ${timeoutMinutes} minutes`);
  }
}

async function readFile(base: string, path: string): Promise<string | undefined> {
  try {
    const r = await api(`/filesystem/${encodeURIComponent(path)}`, { base });
    return typeof r === "string" ? r : typeof r?.content === "string" ? r.content : undefined;
  } catch {
    return undefined;
  }
}

export async function runInSandbox(opts: SandboxOptions): Promise<boolean> {
  credentials();
  const started = Date.now();
  const scratch = mkdtempSync(join(tmpdir(), "rh-pipe-sandbox-"));
  const { sha, file } = await archive(scratch);
  const name = `rh-pipe-${sha.slice(0, 12)}-${Math.random().toString(36).slice(2, 6)}`;
  console.log(`sandbox ${name}: snapshot ${sha}`);
  let ok = false;
  try {
    // The image's Docker runs on vfs over the in-memory root filesystem, and building the services image there
    // fills a 32 GB sandbox (measured 2026-11-12). Docker's data goes on a disk-backed ephemeral volume instead.
    await api("/sandboxes", { method: "POST", body: JSON.stringify({
      metadata: { name, labels: { purpose: "pipeline-e2e" } },
      spec: {
        runtime: { image: IMAGE, memory: MEMORY_MB, ttl: "2h" },
        volumes: [{ name: "docker-data", mountPath: "/var/lib/docker", type: "ephemeral", sizeMb: DOCKER_DISK_MB }],
      },
    }) });
    let sandbox: any;
    for (let i = 0; i < 120; i++) {
      sandbox = await api(`/sandboxes/${name}`);
      if (sandbox.status === "DEPLOYED") break;
      if (["FAILED", "DELETING", "TERMINATED"].includes(sandbox.status)) throw new Error(`sandbox ${sandbox.status}`);
      await new Promise((r) => setTimeout(r, 5_000));
    }
    const base = String(sandbox?.metadata?.url ?? "");
    if (!base) throw new Error("sandbox has no URL after 10 minutes");
    await upload(base, "/tmp/repo.tgz", readFileSync(file));
    const steps = opts.steps?.length ? ` --steps '${opts.steps.join(",").replace(/[^a-z0-9.,_]/g, "")}'` : "";
    const run = await exec(base, "pipeline-e2e", `sh -c 'mkdir -p /work && tar xzf /tmp/repo.tgz -C /work && sh /work/e2e/pipeline/sandbox-run.sh${steps}'`, RUN_MINUTES);
    console.log(run.logs.split("\n").slice(-80).join("\n"));
    mkdirSync(opts.reportDir, { recursive: true });
    const md = await readFile(base, "/work/.relayflow/pipeline-e2e/report.md");
    const json = await readFile(base, "/work/.relayflow/pipeline-e2e/report.json");
    if (md) writeFileSync(join(opts.reportDir, "report.md"), md.replace(/^Target: local\./m, `Target: sandbox (${name}, ${IMAGE}, ${MEMORY_MB} MB; ${Math.round((Date.now() - started) / 1000)}s end to end).`));
    if (json) writeFileSync(join(opts.reportDir, "report.json"), json);
    ok = run.exitCode === 0 && Boolean(md);
    if (!md) console.error("the sandbox run wrote no report");
  } finally {
    rmSync(scratch, { recursive: true, force: true });
    await api(`/sandboxes/${name}`, { method: "DELETE" }).catch((e) => console.error(`could not delete sandbox ${name}: ${(e as Error).message}`));
  }
  return ok;
}
