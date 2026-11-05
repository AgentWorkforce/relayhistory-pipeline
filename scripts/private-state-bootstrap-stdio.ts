/** Private RAM-only IPC. Stdout belongs to the broker; never print errors/credentials. */
import { type OnePasswordPort } from "./private-state-bootstrap.ts";

export const MAX_PRIVATE_FRAME = 20 * 1024 * 1024;
const ACCOUNT = "f7232cb80f6fab86a95426302af243e4";
type Json = Record<string, any>;
const object = (v: any): v is Json => v !== null && typeof v === "object" && !Array.isArray(v);
const keys = (v: any, expected: string) => object(v) && Object.keys(v).sort().join(",") === expected;
const refused = () => new Error("state_bootstrap_private_transport_refused");

export interface PrivateIO {
  input: AsyncIterable<Uint8Array>;
  write(line: string): void;
  close(): void;
}
export class PrivateFrames {
  readonly abort = new AbortController();
  private queued: Json[] = [];
  private pending?: { resolve: (v: Json) => void; reject: (e: Error) => void };
  private ended = false;
  private done = false;
  constructor(private io: PrivateIO, private limit = MAX_PRIVATE_FRAME) {
    void this.pump();
  }
  private fail() {
    if (this.done) return;
    this.ended = true;
    this.abort.abort();
    this.pending?.reject(refused());
    this.pending = undefined;
  }
  private async pump() {
    let chunks: Buffer[] = [], bytes = 0;
    try {
      for await (const chunk of this.io.input) {
        if (this.done) return;
        const data = Buffer.from(chunk);
        let start = 0;
        for (let i = 0; i < data.length; i++) {
          if (data[i] !== 10) continue;
          const tail = data.subarray(start, i);
          if (bytes + tail.length > this.limit) throw refused();
          const joined = Buffer.concat([...chunks, tail], bytes + tail.length);
          const text = new TextDecoder("utf-8", { fatal: true }).decode(joined);
          const value: unknown = JSON.parse(text);
          if (!object(value)) throw refused();
          chunks = []; bytes = 0; start = i + 1;
          if (keys(value, "kind") && value.kind === "stop") {
            this.abort.abort();
            continue; // Keep same-item partial-recovery replies available.
          }
          if (this.pending) {
            const waiting = this.pending; this.pending = undefined; waiting.resolve(value);
          } else {
            if (this.queued.length) throw refused();
            this.queued.push(value);
          }
        }
        if (data.length > start) {
          const tail = data.subarray(start);
          if (bytes + tail.length > this.limit) throw refused();
          chunks.push(Buffer.from(tail)); bytes += tail.length;
        }
      }
      if (!this.done) this.fail(); // EOF aborts in-flight fetch, even between RPCs.
    } catch { this.fail(); }
  }
  async next(timeout = 100_000): Promise<Json> {
    if (this.ended) throw refused();
    if (this.queued.length) return this.queued.shift()!;
    if (this.pending) throw refused();
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      return await new Promise<Json>((resolve, reject) => {
        this.pending = { resolve, reject };
        timer = setTimeout(() => this.fail(), timeout);
      });
    } finally { if (timer) clearTimeout(timer); }
  }
  send(value: unknown) {
    if (this.ended) throw refused();
    const line = JSON.stringify(value);
    if (Buffer.byteLength(line) > MAX_PRIVATE_FRAME) throw refused();
    this.io.write(line + "\n");
  }
  finish() { this.done = true; this.io.close(); }
}

export async function runPrivateBootstrap(
  io: PrivateIO,
  argv: string[],
  execute: (argv: string[], port: OnePasswordPort, signal: AbortSignal) => Promise<unknown>,
) {
  const frames = new PrivateFrames(io);
  try {
    const input = await frames.next();
    if (!keys(input, "accountId,apiToken") || input.accountId !== ACCOUNT ||
      typeof input.apiToken !== "string" || !/^[\x21-\x7e]{16,8192}$/u.test(input.apiToken))
      throw refused();
    frames.abort.signal.throwIfAborted();
    let seq = 0;
    const port: OnePasswordPort = async (args, value) => {
      if (++seq > 40) throw refused();
      frames.send({ kind: "one_password", id: seq, args, input: value ?? null });
      const reply = await frames.next();
      if (!keys(reply, "id,ok,value") || reply.id !== seq || reply.ok !== true)
        throw refused();
      return reply.value;
    };
    const previous = [process.env.CI, process.env.CLOUDFLARE_ACCOUNT_ID, process.env.CLOUDFLARE_API_TOKEN];
    process.env.CI = "true";
    process.env.CLOUDFLARE_ACCOUNT_ID = ACCOUNT;
    process.env.CLOUDFLARE_API_TOKEN = input.apiToken;
    try {
      const result = await execute(argv, port, frames.abort.signal);
      frames.abort.signal.throwIfAborted();
      frames.send({ kind: "result", value: result });
    } finally {
      for (const [i, name] of ["CI", "CLOUDFLARE_ACCOUNT_ID", "CLOUDFLARE_API_TOKEN"].entries()) {
        if (previous[i] === undefined) delete process.env[name]; else process.env[name] = previous[i];
      }
    }
  } finally { frames.finish(); }
}

export async function bootstrapStdio() {
  return runPrivateBootstrap({
    input: process.stdin,
    write: (line) => { process.stdout.write(line); },
    close: () => { process.stdin.destroy(); },
  }, process.argv.slice(2), async (argv, port, signal) => {
    const original = globalThis.fetch;
    // Abort all native CF requests on closed/malformed IPC, without changing the
    // native allowlist/approval guard or using a credential profile.
    globalThis.fetch = ((input, init) => {
      signal.throwIfAborted();
      return original(input, { ...init, signal: AbortSignal.any([
        signal, ...(init?.signal ? [init.signal] : []),
        ...(input instanceof Request ? [input.signal] : []),
      ]) });
    }) as typeof fetch;
    try {
      const { captureStateBootstrap } = await import("./private-state-bootstrap.ts");
      return await captureStateBootstrap(argv, port);
    } finally { globalThis.fetch = original; }
  });
}
if (import.meta.main) bootstrapStdio().catch(() => {
  process.stdout.write(JSON.stringify({ kind: "refused", code: "state_bootstrap_stdio_refused" }) + "\n");
  process.exitCode = 1;
});
