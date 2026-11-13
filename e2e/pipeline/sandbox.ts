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
/**
 * The whole lane's ceiling inside the sandbox: the image build, the install and the steps. With the 5-minute
 * deploy wait and the upload it stays inside the CI flow's 45-minute lane limit, so the flow's own stop never
 * has to cut a run short (and a stop deletes the sandbox anyway: see the signal handler below).
 */
const RUN_MINUTES = 30;
const DEPLOY_POLLS = 60; // 5-second polls: 5 minutes
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

/**
 * The snapshot commit of the working tree and a gzipped archive of it. The upload leaves this machine, so it
 * passes the same refusals as a push to the public remote (remote-ci.mjs refuseToPublish: machine-local state,
 * secret-like files, production identifiers, including any line added over the merge base with main).
 */
async function archive(dir: string): Promise<{ sha: string; file: string }> {
  // The publish refusals are gate code: run from the gate's flows/lib (lane.sh passes RH_GATE_LIB, the
  // checkout the CI flow was loaded from), and from this tree's copy only when run by hand.
  const gateLib = process.env.RH_GATE_LIB?.trim() || fileURLToPath(new URL("../../flows/lib", import.meta.url));
  const ci = await import(`${gateLib}/remote-ci.mjs`) as { snapshot: () => string; refuseToPublish: (sha: string, base: string | undefined, root: string) => Promise<string> };
  const sha = ci.snapshot();
  let base: string | undefined;
  try { base = execFileSync("git", ["-C", ROOT, "merge-base", "origin/main", sha], { encoding: "utf8" }).trim(); } catch {}
  const refused = await ci.refuseToPublish(sha, base, ROOT);
  if (refused) throw new Error(`not uploaded to the sandbox: ${refused}`);
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
  // A report from an earlier run must never stand for this one.
  for (const f of ["report.md", "report.json"]) rmSync(join(opts.reportDir, f), { force: true });
  const scratch = mkdtempSync(join(tmpdir(), "rh-pipe-sandbox-"));
  const { sha, file } = await archive(scratch);
  const name = `rh-pipe-${sha.slice(0, 12)}-${Math.random().toString(36).slice(2, 6)}`;
  console.log(`sandbox ${name}: snapshot ${sha}`);
  let ok = false;
  // A stop (the CI fleet's SIGTERM, a ^C) deletes the 16 GB microVM now rather than leaving it to its 2-hour ttl.
  const remove = () => api(`/sandboxes/${name}`, { method: "DELETE" }).catch((e) => console.error(`could not delete sandbox ${name}: ${(e as Error).message}`));
  const onSignal = (code: number) => () => { void remove().finally(() => process.exit(code)); };
  process.once("SIGINT", onSignal(130));
  process.once("SIGTERM", onSignal(143));
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
    for (let i = 0; i < DEPLOY_POLLS; i++) {
      sandbox = await api(`/sandboxes/${name}`);
      if (sandbox.status === "DEPLOYED") break;
      if (["FAILED", "DELETING", "TERMINATED"].includes(sandbox.status)) throw new Error(`sandbox ${sandbox.status}`);
      await new Promise((r) => setTimeout(r, 5_000));
    }
    const base = String(sandbox?.metadata?.url ?? "");
    if (!base || sandbox?.status !== "DEPLOYED") throw new Error("sandbox not deployed after 5 minutes");
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
    await remove();
  }
  return ok;
}
