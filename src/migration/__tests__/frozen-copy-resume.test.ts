// src/migration/__tests__/frozen-copy-resume.test.ts (pure; no DB, CLI or network)
import { describe, expect, test } from "bun:test";
import { type Binding, type Fs, type Key, checkResumeDir, checkpointName, decide, makeCheckpoint, parseCheckpoint, readCheckpoint,
  sha256, writeCheckpoint, headsSha256, collationSha256 } from "../frozen-copy-resume.ts";

const B: Binding = { project: "pyamkrhoyszgzirqqffq", epoch: "e".repeat(64), privacyBaseline: "p".repeat(64), collation: "c".repeat(64),
  headsSha256: "h".repeat(64), pageQuery: "neon-page-v1" };
const ORG = "0539eb82-a2ad-4f8e-ba5f-c3543f25008d";
const row = (k: Key, seq = 7) => JSON.stringify([ORG, ...k, "rev", "1", "d", "k", "s", "sess", "op", "u", String(seq), String(seq + 1)]);

describe("checkpoint shape and decision", () => {
  const cp = makeCheckpoint(B, "record", ORG, 3, 15000, ["w", "o", "r9"], row(["w", "o", "r9"]), false);
  test("round trip", () => { expect(parseCheckpoint(JSON.stringify(cp), "record", ORG)).toEqual(cp); });
  test("strict parse", () => {
    for (const bad of [{ ...cp, extra: 1 }, { ...cp, v: 2 }, { ...cp, stream: "receipt" }, { ...cp, org: "x" }, { ...cp, pages: 0 },
      { ...cp, tail: ["a", "b"] }, { ...cp, tailRowSha256: "X" }, { ...cp, complete: "no" }, { ...cp, collation: "" }])
      expect(parseCheckpoint(JSON.stringify(bad), "record", ORG)).toBeNull();
    expect(parseCheckpoint("{", "record", ORG)).toBeNull();
    expect(parseCheckpoint(JSON.stringify(cp), "receipt", ORG)).toBeNull();
  });
  test("resume only on full agreement; everything else rescans from the empty key", () => {
    const t = row(["w", "o", "r9"]);
    expect(decide(cp, B, t, t)).toEqual({ from: "checkpoint", tail: ["w", "o", "r9"], pages: 3, rowsThrough: 15000 });
    // REV 3: a complete=true checkpoint yields exactly the same decision (the flag is never surfaced).
    const done = makeCheckpoint(B, "record", ORG, 3, 15000, ["w", "o", "r9"], t, true);
    expect(decide(done, B, t, t)).toEqual({ from: "checkpoint", tail: ["w", "o", "r9"], pages: 3, rowsThrough: 15000 });
    expect("complete" in (decide(done, B, t, t) as object)).toBe(false);
    expect(decide(null, B, t, t)).toEqual({ from: "start", reason: "no_checkpoint" });
    for (const k of Object.keys(B) as (keyof Binding)[])
      expect(decide(cp, { ...B, [k]: "changed" }, t, t)).toEqual({ from: "start", reason: "binding_changed" });
    expect(decide(cp, B, null, t).from).toBe("start");                        // source lost it: never trust
    expect(decide(cp, B, t, null)).toEqual({ from: "start", reason: "tail_absent_on_target" }); // target restore
    expect(decide(cp, B, t, row(["w", "o", "r9"], 8))).toEqual({ from: "start", reason: "tail_row_differs" }); // accepted_seq differs
    const other = makeCheckpoint(B, "record", ORG, 3, 15000, ["w", "o", "r9"], "something else", false);
    expect(decide(other, B, t, t)).toEqual({ from: "start", reason: "tail_digest_differs" });
  });
  test("fingerprints are order-stable and value-sensitive", () => {
    const h = [{ workspace_id: "w", user_id: "u", version: "3", digest: "d", pending: "false" }];
    expect(headsSha256(h)).toBe(headsSha256([{ ...h[0] }]));
    expect(headsSha256(h)).not.toBe(headsSha256([{ ...h[0]!, version: "4" }]));
    expect(collationSha256([{ a: "C.UTF-8", b: "x" }])).toBe(collationSha256([{ b: "x", a: "C.UTF-8" }]));
    expect(collationSha256([{ a: "C.UTF-8" }])).not.toBe(collationSha256([{ a: "en-US" }]));
  });
});

// ---------------------------------------------------------------- fake fs with crash injection and durability
class Crash extends Error {}
class FakeFs implements Fs {
  durable = new Map<string, string>();   // survives a crash (fsync'd file content + fsync'd directory entry)
  volatile = new Map<string, string>();  // page cache
  open = new Map<number, string>(); fdn = 3; steps = 0; crashAt = -1;
  constructor(public dir = "/r") {}
  private step() { if (this.steps++ === this.crashAt) throw new Crash(); }
  lstatSync() { return { isDirectory: () => true, isSymbolicLink: () => false, uid: 1001, mode: 0o40700 }; }
  openSync(p: string) { this.step(); const fd = this.fdn++; this.open.set(fd, p); if (p !== this.dir) this.volatile.set(p, ""); return fd; }
  writeSync(fd: number, d: string) { this.step(); const p = this.open.get(fd)!; this.volatile.set(p, (this.volatile.get(p) ?? "") + d); return d.length; }
  fsyncSync(fd: number) {
    this.step(); const p = this.open.get(fd)!;
    if (p === this.dir) { for (const [k, v] of this.volatile) if (!k.endsWith(".next")) this.durable.set(k, v); }
    else this.durable.set(p + "#data", this.volatile.get(p) ?? "");
  }
  closeSync(fd: number) { this.open.delete(fd); }
  renameSync(a: string, b: string) {
    this.step();
    // Rename moves only data that was fsync'd under the tmp name (we model the worst case of ordered-data ext4).
    this.volatile.set(b, this.durable.get(a + "#data") ?? ""); this.volatile.delete(a);
  }
  readFileSync(p: string) { return this.durable.get(p) ?? ""; }
  existsSync(p: string) { return this.durable.has(p); }
  crash() { this.volatile = new Map(); this.open = new Map(); }
}

/** Model of the patched copyOrg record stream: Neon order = sorted keys; each page = one atomic INSERT that may also
 * fail with "commit outcome unknown" (committed or not); the checkpoint is written only after a successful INSERT.
 * The model uses the module's real decide/makeCheckpoint/writeCheckpoint/readCheckpoint. */
function run(source: Key[], target: Map<string, string>, fs: FakeFs, page: number, failPage: number, failCommits: boolean,
  honorComplete = false) {
  const cp = readCheckpoint("/r", "record", ORG, fs);
  const tailKey = cp ? cp.tail.join("\0") : "";
  const d = decide(cp, B, cp && source.some((k) => k.join("\0") === tailKey) ? row(cp.tail) : null, cp ? target.get(tailKey) ?? null : null);
  let last = d.from === "checkpoint" ? d.tail.join("\0") : "", pages = d.from === "checkpoint" ? d.pages : 0;
  let through = d.from === "checkpoint" ? d.rowsThrough : 0;
  // REV 3: the patched copy never honors `complete`; `honorComplete` models the rev 2 bug for the negative test only.
  if (honorComplete && d.from === "checkpoint" && cp?.complete) return d;
  for (let p = 0; ; p++) {
    const rows = source.filter((k) => k.join("\0") > last).slice(0, page);  // C.UTF-8 = codepoint order
    if (!rows.length) break;
    if (p === failPage) {                       // the INSERT rejects: outcome unknown to the client
      if (failCommits) for (const k of rows) if (!target.has(k.join("\0"))) target.set(k.join("\0"), row(k));
      throw new Crash();
    }
    for (const k of rows) if (!target.has(k.join("\0"))) target.set(k.join("\0"), row(k)); // ON CONFLICT DO NOTHING
    const t = rows[rows.length - 1]!; pages++; through += rows.length; last = t.join("\0");
    writeCheckpoint("/r", makeCheckpoint(B, "record", ORG, pages, through, t, row(t), false), fs);
    if (rows.length < page) break;
  }
  if (pages) {  // the live scan observed EOF after the tail: informational complete marker, as the patch writes
    const t = last.split("\0") as Key;
    writeCheckpoint("/r", makeCheckpoint(B, "record", ORG, pages, through, t, row(t), true), fs);
  }
  return d;
}

/** Overwrite the durable checkpoint, as an attacker or bug with write access to the private dir could. */
function tamper(fs: FakeFs, edit: (cp: any) => any) {
  const p = "/r/" + checkpointName("record", ORG);
  fs.durable.set(p, JSON.stringify(edit(JSON.parse(fs.durable.get(p)!))) + "\n");
}

describe("crash-injection: commit gaps are rescanned, never skipped", () => {
  const source: Key[] = [];
  for (let w = 0; w < 3; w++) for (let o = 0; o < 4; o++) for (let r = 0; r < 9; r++) source.push([`w${w}`, `o${o}`, `r${String(r).padStart(2, "0")}`]);
  source.sort((a, b) => (a.join("\0") < b.join("\0") ? -1 : 1));
  test("every crash point x every failing page x commit outcome converges to the full source", () => {
    let scenarios = 0;
    for (const page of [1, 5, 7, 108]) for (let crashAt = -1; crashAt < 60; crashAt++) for (let failPage = -1; failPage < 6; failPage++)
      for (const failCommits of [false, true]) {
        const fs = new FakeFs(), target = new Map<string, string>();
        fs.crashAt = crashAt;
        let ok = false;
        for (let attempt = 0; attempt < 400 && !ok; attempt++) {
          try { run(source, target, fs, page, attempt === 0 ? failPage : -1, failCommits); ok = true; }
          catch (e) { if (!(e instanceof Crash)) throw e; fs.crash(); fs.crashAt = -1; }
        }
        expect(ok).toBe(true);
        // Never skipped: the target holds exactly the source (and each value is the source's, accepted_seq included).
        expect([...target.keys()].sort()).toEqual(source.map((k) => k.join("\0")));
        for (const k of source) expect(target.get(k.join("\0"))).toBe(row(k));
        // A durable checkpoint is never ahead of committed data.
        const cp = readCheckpoint("/r", "record", ORG, fs);
        if (cp) expect(target.has(cp.tail.join("\0"))).toBe(true);
        scenarios++;
      }
    expect(scenarios).toBe(4 * 61 * 7 * 2);
  });
  test("target data loss after a checkpoint (restore) forces a rescan from the empty key", () => {
    const fs = new FakeFs(), target = new Map<string, string>();
    run(source, target, fs, 5, -1, false);
    const restored = new Map([...target].slice(0, 10)); // a consistent earlier snapshot: a prefix
    const d = run(source, restored, fs, 5, -1, false);
    expect(d).toEqual({ from: "start", reason: "tail_absent_on_target" });
    expect([...restored.keys()].sort()).toEqual(source.map((k) => k.join("\0")));
  });
  test("REV 3 negative: a valid intermediate checkpoint flipped to complete=true skips NOTHING", () => {
    for (const page of [1, 5, 7]) {
      const fs = new FakeFs(), target = new Map<string, string>();
      fs.crashAt = 40;  // crash mid-copy so only an intermediate checkpoint exists
      try { run(source, target, fs, page, -1, false); } catch (e) { if (!(e instanceof Crash)) throw e; fs.crash(); fs.crashAt = -1; }
      const before = readCheckpoint("/r", "record", ORG, fs)!;
      expect(before.complete).toBe(false);
      expect(target.size).toBeLessThan(source.length);
      tamper(fs, (cp) => ({ ...cp, complete: true }));  // bindings, digest and tail all still valid
      expect(readCheckpoint("/r", "record", ORG, fs)!.complete).toBe(true);
      // The rev 2 behaviour (honoring the flag) really would skip rows: the vulnerability the review found.
      const bugTarget = new Map(target), bugFs = Object.assign(new FakeFs(), { durable: new Map(fs.durable) });
      run(source, bugTarget, bugFs, page, -1, false, true);
      expect(bugTarget.size).toBeLessThan(source.length);
      // The rev 3 path resumes from the tail and the live Neon successor query copies the rest.
      run(source, target, fs, page, -1, false);
      expect([...target.keys()].sort()).toEqual(source.map((k) => k.join("\0")));
    }
  });
  test("REV 3 negative: tampered tail pointing past committed data rescans from the empty key", () => {
    const fs = new FakeFs(), target = new Map<string, string>();
    fs.crashAt = 30;
    try { run(source, target, fs, 5, -1, false); } catch (e) { if (!(e instanceof Crash)) throw e; fs.crash(); fs.crashAt = -1; }
    const later = source[source.length - 1]!;
    tamper(fs, (cp) => ({ ...cp, tail: later, tailRowSha256: sha256(row(later)), complete: true }));
    const d = run(source, target, fs, 5, -1, false);
    expect(d).toEqual({ from: "start", reason: "tail_absent_on_target" });
    expect([...target.keys()].sort()).toEqual(source.map((k) => k.join("\0")));
  });
  test("checkpoint file name hides the org id; dir must be private", () => {
    expect(checkpointName("record", ORG)).toBe(sha256("record\0" + ORG) + ".json");
    expect(checkpointName("record", ORG)).not.toContain(ORG);
    const fs = new FakeFs();
    expect(() => checkResumeDir("/r", fs, 1001)).not.toThrow();
    expect(() => checkResumeDir("/r", fs, 1000)).toThrow("resume_dir_refused");
    expect(() => checkResumeDir("relative", fs, 1001)).toThrow("resume_dir_refused");
    expect(() => checkResumeDir("/a/../b", fs, 1001)).toThrow("resume_dir_refused");
    const loose = Object.assign(new FakeFs(), { lstatSync: () => ({ isDirectory: () => true, isSymbolicLink: () => false, uid: 1001, mode: 0o40755 }) });
    expect(() => checkResumeDir("/r", loose, 1001)).toThrow("resume_dir_refused");
  });
});
