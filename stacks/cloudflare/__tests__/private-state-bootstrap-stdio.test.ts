import { expect, test } from "bun:test";
import { PrivateFrames, runPrivateBootstrap, type PrivateIO } from "../../../scripts/private-state-bootstrap-stdio.ts";

class Queue implements AsyncIterable<Uint8Array> {
  values: (Uint8Array | null)[] = [];
  wake?: (v: IteratorResult<Uint8Array>) => void;
  push(value: object | string) {
    this.raw(Buffer.from(typeof value === "string" ? value : JSON.stringify(value) + "\n"));
  }
  raw(value: Uint8Array) {
    if (this.wake) { const wake = this.wake; this.wake = undefined; wake({ done: false, value }); }
    else this.values.push(value);
  }
  end() {
    if (this.wake) { const wake = this.wake; this.wake = undefined; wake({ done: true, value: undefined }); }
    else this.values.push(null);
  }
  [Symbol.asyncIterator]() { return { next: async (): Promise<IteratorResult<Uint8Array>> => {
    if (this.values.length) { const value = this.values.shift(); return value === null ?
      { done: true, value: undefined } : { done: false, value: value! }; }
    return new Promise((resolve) => { this.wake = resolve; });
  } }; }
}
const initial = { accountId: "f7232cb80f6fab86a95426302af243e4", apiToken: "synthetic-bearer-abcdefghijkl" };
function io() {
  const input = new Queue(), output: any[] = [];
  let closed = false;
  const port: PrivateIO = { input, write: (line) => output.push(JSON.parse(line)),
    close: () => { closed = true; input.end(); } };
  return { input, output, port, closed: () => closed };
}
const turn = () => new Promise((r) => setTimeout(r, 0));

test("private frames reject overbound bytes before newline/JSON allocation", async () => {
  const q = io(), frames = new PrivateFrames(q.port, 8);
  const result = frames.next();
  q.input.push("123456789");
  await expect(result).rejects.toThrow();
  expect(frames.abort.signal.aborted).toBe(true);
  frames.finish();
});

test("EOF, invalid UTF8 and invalid JSON abort in-flight work", async () => {
  for (const value of [null, Buffer.from([255, 10]), Buffer.from("{\n")]) {
    const q = io(), frames = new PrivateFrames(q.port);
    const result = frames.next();
    if (value === null) q.input.end(); else q.input.raw(value);
    await expect(result).rejects.toThrow();
    expect(frames.abort.signal.aborted).toBe(true);
    frames.finish();
  }
});

test("frame deadline aborts without a provider invocation", async () => {
  const q = io(), frames = new PrivateFrames(q.port);
  await expect(frames.next(5)).rejects.toThrow();
  expect(frames.abort.signal.aborted).toBe(true);
  frames.finish();
});

test("private credentials exact account/shape and no control characters", async () => {
  for (const value of [{ ...initial, accountId: "x" }, { ...initial, extra: true },
    { ...initial, apiToken: "token-with-newline\n" }]) {
    const q = io(); q.input.push(value);
    let invoked = false;
    await expect(runPrivateBootstrap(q.port, [], async () => { invoked = true; })).rejects.toThrow();
    expect(invoked).toBe(false); expect(q.closed()).toBe(true);
  }
});

test("one password reply IDs and shape are strict; process credentials restored", async () => {
  const before = process.env.CLOUDFLARE_API_TOKEN;
  const q = io(); q.input.push(initial);
  const run = runPrivateBootstrap(q.port, [], async (_argv, port) => port(["synthetic"], "pending"));
  await turn();
  expect(q.output[0]).toMatchObject({ kind: "one_password", id: 1, input: "pending" });
  q.input.push({ id: 2, ok: true, value: {} });
  await expect(run).rejects.toThrow();
  expect(process.env.CLOUDFLARE_API_TOKEN).toBe(before);
  expect(q.output.some((f) => f.kind === "result")).toBe(false);
});

test("stop aborts CF execution while permitting same-item recovery reply", async () => {
  const q = io(); q.input.push(initial);
  let aborted = false;
  const run = runPrivateBootstrap(q.port, [], async (_argv, port, signal) => {
    await new Promise<void>((resolve) => signal.addEventListener("abort", () => { aborted = true; resolve(); }, { once: true }));
    return port(["item", "edit", "synthetic-new-item"], "partial");
  });
  await turn(); q.input.push({ kind: "stop" }); await turn();
  expect(aborted).toBe(true);
  expect(q.output[0].kind).toBe("one_password");
  q.input.push({ id: 1, ok: true, value: { recovered: true } });
  await expect(run).rejects.toThrow();
  expect(q.output.some((f) => f.kind === "result")).toBe(false);
});

test("EOF during execution aborts and never emits a success result", async () => {
  const q = io(); q.input.push(initial);
  const run = runPrivateBootstrap(q.port, [], async (_argv, _port, signal) => {
    await new Promise<void>((resolve) => signal.addEventListener("abort", () => resolve(), { once: true }));
    return { operation: "not-real" };
  });
  await turn(); q.input.end();
  await expect(run).rejects.toThrow();
  expect(q.output).toEqual([]);
});
