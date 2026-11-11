/**
 * Release provenance for the vendored relayscrubber package (docs/scrub-engine.md,
 * "Cutover: pinned relayscrubber 0.5.0-alpha.4", "Provenance").
 *
 * Node-only, build/pin-time tooling: nothing in the runtime imports this file. It proves
 * that the bytes vendored under packages/pipeline/scrub-bindgen/ are exactly the
 * `package/` entries of the release tarball the pin names:
 *   (a) sha256(tarball) equals `pin.release.tarball_sha256`;
 *   (b) the archive's package/package.json has name == pin.package and
 *       version == pin.package_version;
 *   (c) every vendored file is byte-equal to its `package/<path>` entry (and present);
 *   (d) the archive holds nothing beyond package/package.json and the vendored set.
 *
 * The tarball is DATA. It is gunzipped and parsed in memory by the small ustar reader
 * below; nothing is extracted to disk and nothing from it is executed or imported.
 * Links, devices, FIFOs, GNU sparse/multivolume entries, global pax headers, absolute
 * paths, `..` segments, duplicate paths and bad header checksums are all refused.
 */
import { createHash } from "node:crypto";
import { lstatSync, readdirSync, readFileSync } from "node:fs";
import { join, relative } from "node:path";
import { gunzipSync } from "node:zlib";

/** Files in the vendored directory that are repository-local, not part of the release. */
export const REPO_LOCAL_VENDORED_FILES: ReadonlySet<string> = new Set([".gitattributes"]);
/** Upper bound for the decompressed archive (the real one is about 1.8 MB). */
const MAX_TAR_BYTES = 64 << 20;
const BLOCK = 512;

export const sha256Hex = (bytes: Uint8Array): string => createHash("sha256").update(bytes).digest("hex");

export class TarFormatError extends Error {
  constructor(message: string) {
    super(`release tarball rejected: ${message}`);
    this.name = "TarFormatError";
  }
}

function field(header: Uint8Array, offset: number, length: number): string {
  const slice = header.subarray(offset, offset + length);
  const end = slice.indexOf(0);
  return Buffer.from(end === -1 ? slice : slice.subarray(0, end)).toString("utf8");
}

function octal(header: Uint8Array, offset: number, length: number, what: string): number {
  if ((header[offset]! & 0x80) !== 0) throw new TarFormatError(`${what} uses base-256 encoding`);
  const text = field(header, offset, length).trim();
  if (text === "") return 0;
  if (!/^[0-7]+$/.test(text)) throw new TarFormatError(`${what} is not octal`);
  const value = Number.parseInt(text, 8);
  if (!Number.isSafeInteger(value)) throw new TarFormatError(`${what} is out of range`);
  return value;
}

function checksumOk(header: Uint8Array): boolean {
  const want = octal(header, 148, 8, "header checksum");
  let sum = 0;
  for (let i = 0; i < BLOCK; i++) sum += i >= 148 && i < 156 ? 0x20 : header[i]!;
  return sum === want;
}

/** pax extended header records: "<len> <key>=<value>\n". */
function parsePax(body: Uint8Array): Record<string, string> {
  const out: Record<string, string> = {};
  let at = 0;
  while (at < body.length) {
    const space = body.indexOf(0x20, at);
    if (space === -1) throw new TarFormatError("malformed pax record");
    const len = Number.parseInt(Buffer.from(body.subarray(at, space)).toString("ascii"), 10);
    if (!Number.isSafeInteger(len) || len <= space - at || at + len > body.length) throw new TarFormatError("malformed pax record");
    const record = Buffer.from(body.subarray(space + 1, at + len)).toString("utf8");
    if (!record.endsWith("\n")) throw new TarFormatError("malformed pax record");
    const eq = record.indexOf("=");
    if (eq <= 0) throw new TarFormatError("malformed pax record");
    out[record.slice(0, eq)] = record.slice(eq + 1, -1);
    at += len;
  }
  return out;
}

/** `package/a/b` with no empty, `.` or `..` segment, no backslash, NUL or drive colon. */
function safeEntryPath(raw: string): string {
  const path = raw.replace(/\/+$/, "");
  if (path === "" || path.startsWith("/") || /[\\\0:]/.test(path)) throw new TarFormatError(`unsafe entry path ${JSON.stringify(raw)}`);
  if (!path.split("/").every((seg) => seg !== "" && seg !== "." && seg !== ".."))
    throw new TarFormatError(`unsafe entry path ${JSON.stringify(raw)}`);
  return path;
}

/**
 * Regular files of a .tgz (or plain .tar), keyed by archive path. Directory entries are
 * accepted and skipped; every other entry kind is refused.
 */
export function readTarGz(archive: Uint8Array): Map<string, Uint8Array> {
  let tar: Uint8Array;
  if (archive[0] === 0x1f && archive[1] === 0x8b) {
    try {
      tar = gunzipSync(archive, { maxOutputLength: MAX_TAR_BYTES });
    } catch (error) {
      throw new TarFormatError(`gunzip failed (${(error as Error).message})`);
    }
  } else {
    tar = archive;
  }
  const files = new Map<string, Uint8Array>();
  let pax: Record<string, string> = {};
  let longName: string | undefined;
  let at = 0;
  for (;;) {
    if (at + BLOCK > tar.length) throw new TarFormatError("truncated archive (no end-of-archive marker)");
    const header = tar.subarray(at, at + BLOCK);
    if (header.every((b) => b === 0)) break; // end of archive
    if (!checksumOk(header)) throw new TarFormatError(`bad header checksum at offset ${at}`);
    const type = String.fromCharCode(header[156]!);
    let size = octal(header, 124, 12, "entry size");
    if (pax.size !== undefined) {
      if (!/^[0-9]+$/.test(pax.size)) throw new TarFormatError("malformed pax size");
      size = Number.parseInt(pax.size, 10);
    }
    const bodyStart = at + BLOCK;
    const bodyEnd = bodyStart + size;
    if (!Number.isSafeInteger(bodyEnd) || bodyEnd > tar.length) throw new TarFormatError("entry body runs past the archive end");
    const body = tar.subarray(bodyStart, bodyEnd);
    at = bodyStart + Math.ceil(size / BLOCK) * BLOCK;

    if (type === "x") {
      pax = parsePax(body);
      continue;
    }
    if (type === "L") {
      longName = field(body, 0, body.length);
      continue;
    }
    if (type === "g") throw new TarFormatError("global pax header");
    const ustar = field(header, 257, 6).startsWith("ustar");
    const prefix = ustar ? field(header, 345, 155) : "";
    const name = pax.path ?? longName ?? (prefix ? `${prefix}/${field(header, 0, 100)}` : field(header, 0, 100));
    pax = {};
    longName = undefined;
    const path = safeEntryPath(name);
    if (type === "5") continue;
    if (type !== "0" && type !== "\0" && type !== "7")
      throw new TarFormatError(`entry ${JSON.stringify(path)} has unsupported type ${JSON.stringify(type)}`);
    if (type === "7") throw new TarFormatError(`entry ${JSON.stringify(path)} is a contiguous file`);
    if (files.has(path)) throw new TarFormatError(`duplicate entry ${JSON.stringify(path)}`);
    files.set(path, Uint8Array.from(body));
  }
  return files;
}

/** Vendored files (relative, `/`-separated), excluding repository-local files. Symlinks are refused. */
export function listVendoredFiles(dir: string): string[] {
  const out: string[] = [];
  const walk = (d: string) => {
    for (const name of readdirSync(d)) {
      const full = join(d, name);
      const st = lstatSync(full);
      const rel = relative(dir, full).split("\\").join("/");
      if (st.isSymbolicLink()) throw new Error(`vendored ${rel} is a symlink`);
      if (st.isDirectory()) walk(full);
      else if (st.isFile()) {
        if (!REPO_LOCAL_VENDORED_FILES.has(rel)) out.push(rel);
      } else throw new Error(`vendored ${rel} is not a regular file`);
    }
  };
  walk(dir);
  return out.sort();
}

export interface ProvenancePin {
  package: string;
  package_version: string;
  release: { tarball_sha256: string; [key: string]: unknown };
}

export interface ProvenanceResult {
  ok: boolean;
  mismatches: string[];
  /** sha256 of the supplied tarball. */
  tarballSha256: string;
  /** sha256 of every regular `package/` entry, keyed by its path inside `package/`. */
  entries: Record<string, string>;
}

/**
 * Verify `tarball` against `pin.release` and the vendored directory (checks (a)–(d) above).
 * Never throws for a mismatch or a malformed archive: those are `mismatches`.
 */
export function verifyReleaseTarball(input: { tarball: Uint8Array; pin: ProvenancePin; vendoredDir: string }): ProvenanceResult {
  const mismatches: string[] = [];
  const tarballSha256 = sha256Hex(input.tarball);
  const entries: Record<string, string> = {};
  if (tarballSha256 !== input.pin.release?.tarball_sha256)
    mismatches.push(`tarball sha256 ${tarballSha256} != pin release.tarball_sha256 ${String(input.pin.release?.tarball_sha256)}`);

  let files: Map<string, Uint8Array>;
  try {
    files = readTarGz(input.tarball);
  } catch (error) {
    mismatches.push((error as Error).message);
    return { ok: false, mismatches, tarballSha256, entries };
  }
  const pkg = new Map<string, Uint8Array>();
  for (const [path, bytes] of files) {
    if (!path.startsWith("package/")) {
      mismatches.push(`archive entry outside package/: ${path}`);
      continue;
    }
    const rel = path.slice("package/".length);
    pkg.set(rel, bytes);
    entries[rel] = sha256Hex(bytes);
  }

  const manifest = pkg.get("package.json");
  if (!manifest) mismatches.push("archive has no package/package.json");
  else {
    let parsed: { name?: unknown; version?: unknown } | undefined;
    try {
      parsed = JSON.parse(Buffer.from(manifest).toString("utf8"));
    } catch {
      mismatches.push("package/package.json is not JSON");
    }
    if (parsed) {
      if (parsed.name !== input.pin.package) mismatches.push(`package.json name ${String(parsed.name)} != pin package ${input.pin.package}`);
      if (parsed.version !== input.pin.package_version)
        mismatches.push(`package.json version ${String(parsed.version)} != pin package_version ${input.pin.package_version}`);
    }
  }

  let vendored: string[] = [];
  try {
    vendored = listVendoredFiles(input.vendoredDir);
  } catch (error) {
    mismatches.push((error as Error).message);
  }
  if (vendored.length === 0) mismatches.push("no vendored files");
  for (const rel of vendored) {
    const entry = pkg.get(rel);
    if (!entry) {
      mismatches.push(`vendored ${rel} is not in the archive`);
      continue;
    }
    const local = readFileSync(join(input.vendoredDir, rel));
    if (!Buffer.from(entry).equals(local)) mismatches.push(`vendored ${rel} sha256 ${sha256Hex(local)} != archive ${entries[rel]}`);
  }
  const allowed = new Set(["package.json", ...vendored]);
  for (const rel of pkg.keys()) if (!allowed.has(rel)) mismatches.push(`archive has an extra file package/${rel}`);

  return { ok: mismatches.length === 0, mismatches, tarballSha256, entries };
}
