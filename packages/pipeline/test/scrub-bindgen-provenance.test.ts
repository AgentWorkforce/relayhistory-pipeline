/**
 * Release provenance for the vendored relayscrubber package (docs/scrub-engine.md,
 * "Provenance"): src/lib/scrub-bindgen-provenance.ts verifies a release tarball against
 * the pin's `release` block and the vendored bytes. Synthetic tarballs only (no network,
 * nothing executed); the real alpha.4 tarball is checked by
 * `bun scripts/scrub-bindgen-receipt.ts --tarball <path>` outside CI.
 */
import { createHash } from "node:crypto";
import { mkdirSync, mkdtempSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { gzipSync } from "node:zlib";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { readTarGz, verifyReleaseTarball, type ProvenancePin } from "../src/lib/scrub-bindgen-provenance.js";

const sha = (b: Uint8Array) => createHash("sha256").update(b).digest("hex");
const enc = (s: string) => new TextEncoder().encode(s);

interface Entry {
  path: string;
  body?: Uint8Array;
  type?: string;
}

/** Minimal ustar writer (with checksums) for synthetic test archives. */
function ustar(entries: Entry[]): Uint8Array {
  const blocks: Uint8Array[] = [];
  for (const e of entries) {
    const body = e.body ?? new Uint8Array();
    const h = new Uint8Array(512);
    const put = (off: number, len: number, s: string) => h.set(enc(s).subarray(0, len), off);
    put(0, 100, e.path);
    put(100, 8, "0000644\0");
    put(108, 8, "0000000\0");
    put(116, 8, "0000000\0");
    put(124, 12, body.length.toString(8).padStart(11, "0") + "\0");
    put(136, 12, "00000000000\0");
    put(156, 1, e.type ?? "0");
    put(257, 6, "ustar\0");
    put(263, 2, "00");
    h.fill(0x20, 148, 156);
    const sum = h.reduce((a, b) => a + b, 0);
    put(148, 8, sum.toString(8).padStart(6, "0") + "\0 ");
    blocks.push(h);
    if (body.length > 0) {
      const padded = new Uint8Array(Math.ceil(body.length / 512) * 512);
      padded.set(body);
      blocks.push(padded);
    }
  }
  blocks.push(new Uint8Array(1024));
  const out = new Uint8Array(blocks.reduce((n, b) => n + b.length, 0));
  let at = 0;
  for (const b of blocks) {
    out.set(b, at);
    at += b.length;
  }
  return out;
}

const VENDORED: Record<string, Uint8Array> = {
  "index.js": enc("export * from './generated/relayscrubber.js';\n"),
  "index.d.ts": enc("export {};\n"),
  "generated/artifact.json": enc('{"manifest":{}}\n'),
  "generated/relayscrubber.js": enc("// glue\n"),
  "generated/relayscrubber_bg.wasm": Uint8Array.from([0x00, 0x61, 0x73, 0x6d, 1, 0, 0, 0]),
};
const PKG_JSON = enc(JSON.stringify({ name: "@agentworkforce/relayscrubber", version: "0.5.0-test.1" }));

function packageEntries(over: Partial<Record<string, Uint8Array>> = {}): Entry[] {
  const files: Record<string, Uint8Array | undefined> = { "package.json": PKG_JSON, ...VENDORED, ...over };
  return Object.entries(files)
    .filter((kv): kv is [string, Uint8Array] => kv[1] !== undefined)
    .map(([rel, body]) => ({ path: `package/${rel}`, body }));
}

const tgz = (entries: Entry[]) => gzipSync(ustar(entries));

function pinFor(tarball: Uint8Array): ProvenancePin {
  return {
    package: "@agentworkforce/relayscrubber",
    package_version: "0.5.0-test.1",
    release: { tarball_sha256: sha(tarball) },
  };
}

let dir: string;
let vendoredDir: string;
beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "scrub-provenance-"));
  vendoredDir = join(dir, "scrub-bindgen");
  for (const [rel, body] of Object.entries(VENDORED)) {
    mkdirSync(dirname(join(vendoredDir, rel)), { recursive: true });
    writeFileSync(join(vendoredDir, rel), body);
  }
  writeFileSync(join(vendoredDir, ".gitattributes"), "* -text\n");
});
afterEach(() => rmSync(dir, { recursive: true, force: true }));

describe("verifyReleaseTarball", () => {
  it("a matching tarball verifies, with per-entry digests", () => {
    const tarball = tgz(packageEntries());
    const r = verifyReleaseTarball({ tarball, pin: pinFor(tarball), vendoredDir });
    expect(r.mismatches).toEqual([]);
    expect(r.ok).toBe(true);
    expect(r.tarballSha256).toBe(sha(tarball));
    expect(r.entries["generated/relayscrubber_bg.wasm"]).toBe(sha(VENDORED["generated/relayscrubber_bg.wasm"]!));
    expect(Object.keys(r.entries).sort()).toEqual(["package.json", ...Object.keys(VENDORED)].sort());
  });

  it("a mutated wasm byte fails (vendored != archive)", () => {
    const wasm = Uint8Array.from(VENDORED["generated/relayscrubber_bg.wasm"]!);
    wasm[7] ^= 1;
    const tarball = tgz(packageEntries({ "generated/relayscrubber_bg.wasm": wasm }));
    const r = verifyReleaseTarball({ tarball, pin: pinFor(tarball), vendoredDir });
    expect(r.ok).toBe(false);
    expect(r.mismatches).toEqual([expect.stringMatching(/^vendored generated\/relayscrubber_bg\.wasm sha256 /)]);
  });

  it("an extra file in the archive fails", () => {
    const tarball = tgz([...packageEntries(), { path: "package/postinstall.js", body: enc("evil()") }]);
    const r = verifyReleaseTarball({ tarball, pin: pinFor(tarball), vendoredDir });
    expect(r.ok).toBe(false);
    expect(r.mismatches).toEqual(["archive has an extra file package/postinstall.js"]);
  });

  it("an entry outside package/ fails", () => {
    const tarball = tgz([...packageEntries(), { path: "other/x.js", body: enc("x") }]);
    const r = verifyReleaseTarball({ tarball, pin: pinFor(tarball), vendoredDir });
    expect(r.ok).toBe(false);
    expect(r.mismatches).toEqual(["archive entry outside package/: other/x.js"]);
  });

  it("a vendored file missing from the archive fails", () => {
    const tarball = tgz(packageEntries({ "index.d.ts": undefined }));
    const r = verifyReleaseTarball({ tarball, pin: pinFor(tarball), vendoredDir });
    expect(r.ok).toBe(false);
    expect(r.mismatches).toEqual(["vendored index.d.ts is not in the archive"]);
  });

  it("a wrong package.json version fails", () => {
    const pkg = enc(JSON.stringify({ name: "@agentworkforce/relayscrubber", version: "0.5.0-test.2" }));
    const tarball = tgz(packageEntries({ "package.json": pkg }));
    const r = verifyReleaseTarball({ tarball, pin: pinFor(tarball), vendoredDir });
    expect(r.ok).toBe(false);
    expect(r.mismatches).toEqual(["package.json version 0.5.0-test.2 != pin package_version 0.5.0-test.1"]);
  });

  it("a wrong package.json name fails", () => {
    const pkg = enc(JSON.stringify({ name: "relayscrubber-fork", version: "0.5.0-test.1" }));
    const tarball = tgz(packageEntries({ "package.json": pkg }));
    const r = verifyReleaseTarball({ tarball, pin: pinFor(tarball), vendoredDir });
    expect(r.ok).toBe(false);
    expect(r.mismatches).toEqual(["package.json name relayscrubber-fork != pin package @agentworkforce/relayscrubber"]);
  });

  it("a wrong tarball_sha256 in the pin fails", () => {
    const tarball = tgz(packageEntries());
    const pin = { ...pinFor(tarball), release: { tarball_sha256: "0".repeat(64) } };
    const r = verifyReleaseTarball({ tarball, pin, vendoredDir });
    expect(r.ok).toBe(false);
    expect(r.mismatches).toEqual([`tarball sha256 ${sha(tarball)} != pin release.tarball_sha256 ${"0".repeat(64)}`]);
  });

  it("a vendored symlink fails", () => {
    symlinkSync("index.js", join(vendoredDir, "alias.js"));
    const tarball = tgz(packageEntries());
    const r = verifyReleaseTarball({ tarball, pin: pinFor(tarball), vendoredDir });
    expect(r.ok).toBe(false);
    expect(r.mismatches).toContain("vendored alias.js is a symlink");
  });

  it("a malformed archive is a mismatch, not a throw", () => {
    const tarball = gzipSync(enc("not a tar archive"));
    const r = verifyReleaseTarball({ tarball, pin: pinFor(tarball), vendoredDir });
    expect(r.ok).toBe(false);
    expect(r.mismatches[0]).toMatch(/^release tarball rejected: /);
  });
});

describe("readTarGz refuses unsafe entries", () => {
  const cases: Array<[string, Entry[], RegExp]> = [
    ["symlink", [{ path: "package/link", type: "2" }], /unsupported type "2"/],
    ["hardlink", [{ path: "package/link", type: "1" }], /unsupported type "1"/],
    ["parent segment", [{ path: "package/../x", body: enc("x") }], /unsafe entry path/],
    ["absolute path", [{ path: "/etc/x", body: enc("x") }], /unsafe entry path/],
    ["global pax header", [{ path: "pax", type: "g" }], /global pax header/],
    [
      "duplicate entry",
      [
        { path: "package/a", body: enc("1") },
        { path: "package/a", body: enc("2") },
      ],
      /duplicate entry/,
    ],
  ];
  for (const [name, entries, error] of cases) {
    it(name, () => expect(() => readTarGz(tgz(entries))).toThrow(error));
  }

  it("a corrupted header checksum", () => {
    const tar = ustar([{ path: "package/a", body: enc("1") }]);
    tar[0] ^= 1;
    expect(() => readTarGz(tar)).toThrow(/bad header checksum/);
  });

  it("a truncated archive", () => {
    const tar = ustar([{ path: "package/a", body: enc("1") }]);
    expect(() => readTarGz(tar.subarray(0, 512))).toThrow(/runs past|truncated/);
  });

  it("directory entries are skipped; pax path overrides the header name", () => {
    const pax = enc(`${"21 path=package/long\n".length} path=package/long\n`);
    const files = readTarGz(
      tgz([{ path: "package/", type: "5" }, { path: "PaxHeader", type: "x", body: pax }, { path: "short", body: enc("v") }]),
    );
    expect([...files.keys()]).toEqual(["package/long"]);
  });
});
