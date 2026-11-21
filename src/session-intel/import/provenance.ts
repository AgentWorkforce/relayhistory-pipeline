/** Offline action scope evidence. No Git execution, directory probes or inferred canonical repositories. */
import { constants } from "node:fs";
import { open } from "node:fs/promises";
import { posix } from "node:path";
import { ImportError, IMPORT_BOUNDS, type LineRef } from "./normalize.ts";
import { isoTime, isRecord, onlyKeys, sha256Hex, textCompare } from "../replay/util.ts";

export const PROVENANCE_FORMAT = "session-intel-action-provenance/1";
export const PROVENANCE_PATH_FORMAT = "session-intel-action-provenance/2";
/** Names the existing M4 import policy, not a different path vocabulary. */
export const REPOSITORY_PATH_ENCODING = "m4-import-salted-segments/1";
export const PROVENANCE_BOUNDS = { command: 64 * 1024, tokens: 4096, mapBytes: 2 * 1024 * 1024, roots: 10_000, exportBytes: 16 * 1024 * 1024, captureBytes: 16 * 1024 * 1024, actions: 100_000 } as const;
export type DirectorySource = "tool_workdir" | "session_cwd" | "record_cwd" | "generic_event" | "code_mode_projection" | "unknown";
export interface ActionContext {
  directory: string | null;
  startingDirectory: string | null;
  target: string | null;
  source: DirectorySource;
  reason: "literal" | "missing_directory" | "unsupported_shell" | "dynamic_workdir" | "projection_scope_unavailable";
  at: LineRef;
  observedAt: string;
  evidenceAt: string | null;
  evidenceRef: LineRef | null;
  /** A trace declaration at the action's time, never a later session value. */
  declaration: { repository: string; root: string | null; observedAt: string } | null;
}
export interface RepositoryRoot { directory: string; repository: string; observedAt: string }
export interface RepositoryMap { roots: RepositoryRoot[]; sha256: string }
export interface ActionRecord {
  harness: string; sessionId: string; eventId: string; kind: "write" | "commit" | "outcome";
  factAt: string; sourceAt: string; location: string | null; effectiveLocation: string | null; startingLocation: string | null; target: string | null; source: DirectorySource; reason: ActionContext["reason"];
  repository: string | null; repositoryTarget: string | null; repositoryBasis: "operator_declared" | "trace_declared" | "unattributed" | "conflicting_claims";
  repositoryClaims: Array<{ repository: string; basis: "operator_declared" | "trace_declared"; availableAt: string }>;
  availableAt: string; repositoryHint: string | null;
  /** /2 only: the existing import's repoRelativePath + scrubPath vocabulary. Null means unavailable. */
  repositoryPath?: string | null;
}
export interface PrivateActionLocation { directory: string | null; startingDirectory: string | null; target: string | null; repository: string | null; source: LineRef; output: LineRef | null; claims: Array<{ repository: string; basis: "operator_declared" | "trace_declared"; observedAt: string }> }
export interface CaptureBudget { sizes: WeakMap<object, number>; count: number; bytes: number }
export interface ActionExport { records: ActionRecord[]; locations: Record<string, PrivateActionLocation> }

/** Lexical paths only. UNC, URLs, home expansion and non-absolute paths without a base stay unknown. */
export function directoryPath(value: unknown, base: string | null = null): string | null {
  if (typeof value !== "string" || !value || value.length > 16_384 || /[\u0000-\u001f\u007f]/u.test(value) || Buffer.from(value).toString("utf8") !== value) return null;
  const windows = /^[A-Za-z]:[\\/]/u.test(value) || (base !== null && /^[A-Za-z]:\//u.test(base));
  if (!windows && value.includes("\\")) return null;
  const path = windows ? value.replace(/\\/gu, "/") : value;
  if (path.startsWith("//") || path.startsWith("~")) return null;
  if (/^[A-Za-z]:/u.test(path) && !/^[A-Za-z]:\//u.test(path)) return null;
  if (path.startsWith("/") || /^[A-Za-z]:\//u.test(path)) {
    if (windows) {
      if (!/^[A-Za-z]:\//u.test(path)) return null;
      let depth = 0;
      for (const part of path.slice(3).split("/")) {
        if (part === "..") { if (depth === 0) return null; depth--; }
        else if (part && part !== ".") depth++;
      }
    }
    let normalized = posix.normalize(path);
    if (windows && /^[A-Za-z]:$/u.test(normalized)) normalized += "/";
    if (windows && !/^[A-Za-z]:\//u.test(normalized)) return null;
    if (!windows && !normalized.startsWith("/")) return null;
    return /^[A-Za-z]:\/$/u.test(normalized) ? normalized : normalized.replace(/\/$/u, "") || "/";
  }
  return base ? directoryPath(`${base.replace(/\/$/u, "")}/${path}`) : null;
}
const contains = (root: string, path: string) => path === root || path.startsWith(`${root}/`);

/** Canonical identity claims require a host and repository path; a basename is only a hint. */
export function repositoryIdentity(raw: unknown): string | null {
  if (typeof raw !== "string" || raw.length > 4096 || /[\s\u0000-\u001f\u007f%?#;]/u.test(raw)) return null;
  let host: string, path: string;
  try {
    if (raw.includes("://")) {
      const url = new URL(raw);
      if (!["https:", "http:", "ssh:", "git:"].includes(url.protocol) || url.password || (url.username && url.username !== "git") || url.port) return null;
      host = url.hostname; path = url.pathname;
    } else {
      const scp = /^(?:git@)?([A-Za-z0-9.-]+):([^:]+)$/u.exec(raw);
      if (scp) { host = scp[1]!; path = scp[2]!; }
      else { const slash = raw.indexOf("/"); if (slash < 0) return null; host = raw.slice(0, slash); path = raw.slice(slash + 1); }
    }
  } catch { return null; }
  path = path.replace(/^\/+|\/+$/gu, "").replace(/\.git$/iu, "");
  const parts = path.split("/");
  if (!/^[A-Za-z0-9.-]+\.[A-Za-z0-9.-]+$/u.test(host) || parts.length < 2 || parts.some((p) => !/^[A-Za-z0-9._-]+$/u.test(p) || p === "." || p === "..")) return null;
  return `${host.toLowerCase()}/${path}`;
}

/** Read only the operator-selected map file. Never inspect the directories named by a trace or a map. */
export async function readRepositoryMap(path: string, now: Date): Promise<RepositoryMap> {
  let handle;
  try { handle = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK); }
  catch { throw new ImportError("repository map could not be opened as a regular private file"); }
  try {
    const st = await handle.stat();
    if (!st.isFile() || (st.mode & 0o077) !== 0 || st.size > PROVENANCE_BOUNDS.mapBytes) throw new ImportError("repository map must be a private regular file within its size bound");
    const data = Buffer.alloc(PROVENANCE_BOUNDS.mapBytes + 1);
    let bytesRead = 0;
    while (bytesRead < data.length) {
      const read = await handle.read(data, bytesRead, data.length - bytesRead, null);
      if (!read.bytesRead) break;
      bytesRead += read.bytesRead;
    }
    if (bytesRead > PROVENANCE_BOUNDS.mapBytes) throw new ImportError("repository map exceeds its size bound");
    const bytes = data.subarray(0, bytesRead), text = bytes.toString("utf8");
    if (!Buffer.from(text).equals(bytes)) throw new ImportError("repository map must be valid UTF-8; values withheld");
    return parseRepositoryMap(text, now);
  } finally { await handle.close(); }
}
export function parseRepositoryMap(text: string, now: Date): RepositoryMap {
  try {
    if (Buffer.byteLength(text) > PROVENANCE_BOUNDS.mapBytes) throw new Error();
    const raw: unknown = JSON.parse(text);
    if (!isRecord(raw)) throw new Error();
    onlyKeys(raw, ["format", "roots"], "map");
    if (raw.format !== "session-intel-repository-map/1" || !Array.isArray(raw.roots) || raw.roots.length > PROVENANCE_BOUNDS.roots) throw new Error();
    const seen = new Set<string>();
    const roots = raw.roots.map((r): RepositoryRoot => {
      if (!isRecord(r)) throw new Error();
      onlyKeys(r, ["directory", "repository", "observedAt"], "root");
      const directory = directoryPath(r.directory), repository = repositoryIdentity(r.repository), observedAt = isoTime(r.observedAt, "observedAt");
      if (!directory || directory === "/" || /^[A-Za-z]:\/?$/u.test(directory) || !repository || observedAt > now.toISOString() || seen.has(directory)) throw new Error();
      seen.add(directory); return { directory, repository, observedAt };
    }).sort((a, b) => b.directory.length - a.directory.length || textCompare(a.directory, b.directory));
    return { roots, sha256: sha256Hex(text) };
  } catch { throw new ImportError("repository map has an invalid format, root, identity, timestamp or duplicate directory; values withheld"); }
}

export type RepositoryResolver = (path: string) => RepositoryRoot | undefined;
/** One immutable trie per export, not a roots-by-actions quadratic scan. No filesystem operations. */
export function repositoryResolver(roots: readonly RepositoryRoot[]): RepositoryResolver {
  interface Node { children: Map<string, Node>; root?: RepositoryRoot }
  const tree: Node = { children: new Map() };
  for (const value of roots) {
    let at = tree;
    for (const part of value.directory.split("/")) {
      if (!at.children.has(part)) at.children.set(part, { children: new Map() });
      at = at.children.get(part)!;
    }
    if (at.root) throw new ImportError("duplicate action provenance repository root");
    at.root = { ...value };
  }
  return (path) => {
    let at: Node | undefined = tree, found: RepositoryRoot | undefined;
    for (const part of path.split("/")) {
      at = at.children.get(part); if (!at) break;
      if (at.root) found = at.root;
    }
    return found;
  };
}

interface Segment { words: string[]; after: string | null }
/** A deliberately small static shell subset: no substitutions, redirection, pipes or control syntax. */
function segments(script: string): Segment[] | null {
  if (Buffer.byteLength(script) > PROVENANCE_BOUNDS.command) return null;
  const out: Segment[] = []; let words: string[] = [], word = "", active = false, quote = "";
  const endWord = () => { if (active) { words.push(word); word = ""; active = false; } };
  for (let i = 0; i < script.length; i++) {
    const c = script[i]!;
    if (quote === "'") { if (c === "'") quote = ""; else word += c; continue; }
    if (c === "\\") {
      if (++i >= script.length) return null;
      if (script[i] !== "\n") { word += quote === '"' && !'$`"\\'.includes(script[i]!) ? `\\${script[i]}` : script[i]; active = true; }
      continue;
    }
    if (quote === '"') { if (c === '"') quote = ""; else if (c === "$" || c === "`") return null; else word += c; continue; }
    if (c === "'" || c === '"') { quote = c; active = true; continue; }
    if (/[\s]/u.test(c) && c !== "\n") { endWord(); continue; }
    if (c === "\n" || c === ";" || (c === "&" && script[i + 1] === "&")) {
      endWord(); if (words.length) { out.push({ words, after: c === "&" ? "&&" : c }); words = []; }
      if (c === "&") i++; continue;
    }
    if (/[|&$`(){}<>*?\[\]]/u.test(c)) return null;
    word += c; active = true;
    if (out.length + words.length > PROVENANCE_BOUNDS.tokens) return null;
  }
  if (quote) return null;
  endWord(); if (words.length) out.push({ words, after: null });
  return out.length && out.reduce((n, s) => n + s.words.length, 0) <= PROVENANCE_BOUNDS.tokens ? out : null;
}

/** Effective directory for one static Git action. Unsupported/ambiguous scope never falls back to cwd. */
export function gitDirectory(script: string, initial: string | null, action: "commit" | "revert"): string | null {
  const list = segments(script); if (!list) return null;
  let cwd = directoryPath(initial); const found: Array<string | null> = [];
  for (const { words, after } of list) {
    if (words[0] === "cd") {
      if (words.length !== 2 || after !== "&&") return null;
      cwd = directoryPath(words[1], cwd); if (!cwd) return null; continue;
    }
    if (!(words[0] === "git" || words[0]?.endsWith("/git"))) return null;
    let i = 1, local = cwd;
    while (words[i]?.startsWith("-")) {
      const flag = words[i++]!;
      if (flag === "-C") { if (!words[i]) return null; local = directoryPath(words[i++], local); }
      else if (flag.startsWith("-C") && flag.length > 2) local = directoryPath(flag.slice(2), local);
      else if (!["--no-pager", "--no-optional-locks", "--literal-pathspecs"].includes(flag)) return null;
    }
    if (words[i] === action) found.push(local);
  }
  return found.length === 1 ? found[0]! : null;
}

export function actionContext(directory: unknown, source: DirectorySource, at: LineRef, observedAt: string, declaration: ActionContext["declaration"] = null): ActionContext {
  const dir = directoryPath(directory);
  return { directory: dir, startingDirectory: dir, target: null, source, reason: dir ? "literal" : "missing_directory", at: { ...at }, observedAt, evidenceAt: null, evidenceRef: null, declaration };
}

/** No status here means independently verified: these are explicit operator/trace claims only. */
export function actionRecord(context: ActionContext | null, identity: Pick<ActionRecord, "harness" | "sessionId" | "eventId" | "kind" | "factAt">,
  hasher: { location(domain: "record" | "directory" | "target" | "repository-target" | "basename-hint", value: string): string; repo(value: string): string }, roots: readonly RepositoryRoot[] | RepositoryResolver = [],
  options: { encodeRepositoryPath?: (relative: string, root: string) => string | null } = {}): { record: ActionRecord; location: PrivateActionLocation | null } {
  const c = context ?? actionContext(null, "unknown", { file: 0, line: 0 }, identity.factAt);
  const path = c.target ?? c.directory;
  const resolveRoot = typeof roots === "function" ? roots : repositoryResolver(roots);
  const mapped = c.reason === "literal" && path ? resolveRoot(path) : undefined;
  const declared = c.declaration && (c.source === "generic_event" || (c.reason === "literal" && c.directory && c.directory === c.declaration.root
    && (!c.target || (c.declaration.root !== null && contains(c.declaration.root, c.target))))) ? c.declaration : null;
  const claims: PrivateActionLocation["claims"] = [
    ...(mapped ? [{ repository: mapped.repository, basis: "operator_declared" as const, observedAt: mapped.observedAt }] : []),
    ...(declared ? [{ repository: declared.repository, basis: "trace_declared" as const, observedAt: declared.observedAt }] : []),
  ];
  const conflict = mapped && declared && mapped.repository !== declared.repository;
  const repository = conflict ? null : mapped?.repository ?? declared?.repository ?? null;
  const id = context ? hasher.location("record", JSON.stringify([identity.harness, identity.sessionId, identity.eventId, c.directory, c.startingDirectory, c.target, repository, claims, c.at, c.evidenceRef])) : null;
  const hint = c.directory ? posix.basename(c.directory) : null;
  const repositoryPath = options.encodeRepositoryPath && identity.kind === "write" && repository && mapped && c.target
    && c.target !== mapped.directory && contains(mapped.directory, c.target) && c.source !== "code_mode_projection"
    ? options.encodeRepositoryPath(posix.relative(mapped.directory, c.target), mapped.directory) : null;
  return {
    record: { ...identity, sourceAt: c.observedAt, location: id, effectiveLocation: c.directory ? hasher.location("directory", c.directory) : null, startingLocation: c.startingDirectory ? hasher.location("directory", c.startingDirectory) : null, target: c.target ? hasher.location("target", c.target) : null, source: c.source, reason: c.reason,
      repository: repository ? hasher.repo(repository) : null,
      repositoryTarget: repository && mapped && c.target && c.target !== mapped.directory ? hasher.location("repository-target", JSON.stringify([mapped.repository, posix.relative(mapped.directory, c.target)])) : null,
      repositoryBasis: conflict ? "conflicting_claims" : mapped ? "operator_declared" : declared ? "trace_declared" : "unattributed",
      repositoryClaims: claims.map((claim) => ({ repository: hasher.repo(claim.repository), basis: claim.basis, availableAt: claim.observedAt })),
      availableAt: [identity.factAt, c.observedAt, c.evidenceAt ?? c.observedAt, ...claims.map((claim) => claim.observedAt)].sort().at(-1)!,
      repositoryHint: !repository && hint && hint !== "/" && !/^[A-Za-z]:$/u.test(hint) ? hasher.location("basename-hint", hint) : null,
      ...(options.encodeRepositoryPath ? { repositoryPath } : {}) },
    location: id ? { directory: c.directory, startingDirectory: c.startingDirectory, target: c.target, repository, source: { ...c.at }, output: c.evidenceRef ? { ...c.evidenceRef } : null, claims } : null,
  };
}

/** Capture-only extension: existing /1 session records and replay consumers are deliberately untouched. */
export function actionArtifacts(exports: Iterable<ActionExport>, deleteBy: string, repositoryMap: { sha256: string; fingerprint: string } | null, pins: { sessionsSha256: string; implementationSha256: string }, options: { repositoryPaths?: boolean } = {}) {
  const records: ActionRecord[] = [], locations: Record<string, PrivateActionLocation> = Object.create(null);
  let bytes = 0, privateBytes = 0;
  for (const data of exports) {
    for (const record of data.records) {
      if (Object.hasOwn(record, "repositoryPath") !== (options.repositoryPaths === true))
        throw new ImportError("action provenance records must match the requested path format");
      if (options.repositoryPaths && record.repositoryPath !== null && (typeof record.repositoryPath !== "string" || !record.repositoryPath.length || record.repositoryPath.length > IMPORT_BOUNDS.path))
        throw new ImportError("action provenance repository path exceeds its bound or has an invalid type");
      bytes += Buffer.byteLength(JSON.stringify(record)) + 1;
      if (records.length >= PROVENANCE_BOUNDS.actions || bytes > PROVENANCE_BOUNDS.exportBytes)
        throw new ImportError("action provenance export exceeds its bound; use a smaller explicit input frame");
      records.push(record);
    }
    for (const [id, location] of Object.entries(data.locations)) {
      if (Object.hasOwn(locations, id)) {
        if (JSON.stringify(locations[id]) !== JSON.stringify(location)) throw new ImportError("conflicting action provenance location identity");
        continue;
      }
      privateBytes += Buffer.byteLength(JSON.stringify(location)) + id.length + 4;
      if (privateBytes > PROVENANCE_BOUNDS.exportBytes) throw new ImportError("private action provenance map exceeds its bound; use a smaller explicit input frame");
      locations[id] = location;
    }
  }
  const format = options.repositoryPaths ? PROVENANCE_PATH_FORMAT : PROVENANCE_FORMAT;
  const definition = { format, rules: "static-action-scope/1", bounds: PROVENANCE_BOUNDS, identity: "repository-origin-claim/1", status: "capture_only", legacySessionScopeUnchanged: true,
    consumedByReplay: false, trust: "operator-and-trace-claims-not-independent-verification",
    ...(options.repositoryPaths ? { repositoryPathEncoding: REPOSITORY_PATH_ENCODING } : {}) };
  if (![pins.sessionsSha256, pins.implementationSha256].every((pin) => /^[a-f0-9]{64}$/u.test(pin))) throw new ImportError("action provenance requires exact session and implementation digests");
  if (repositoryMap && (!/^[a-f0-9]{64}$/u.test(repositoryMap.sha256) || !/^m_[a-f0-9]{64}$/u.test(repositoryMap.fingerprint)))
    throw new ImportError("action provenance requires a private map digest and salted public fingerprint");
  const counts = { records: records.length, attributedRecords: records.filter((r) => r.repository !== null).length,
    operatorDeclared: records.filter((r) => r.repositoryBasis === "operator_declared").length, traceDeclared: records.filter((r) => r.repositoryBasis === "trace_declared").length,
    unattributed: records.filter((r) => r.repository === null).length, conflictingClaims: records.filter((r) => r.repositoryBasis === "conflicting_claims").length, availableAfterFact: records.filter((r) => r.availableAt > r.factAt).length };
  const body = `${JSON.stringify({ ...definition, ...pins, counts, definitionSha256: sha256Hex(JSON.stringify(definition)), deleteBy, repositoryMapFingerprint: repositoryMap?.fingerprint ?? null, records })}\n`;
  if (Buffer.byteLength(body) > PROVENANCE_BOUNDS.exportBytes) throw new ImportError("action provenance artifact exceeds its bound; use a smaller explicit input frame");
  const privateMapping = { format, repositoryMapSha256: repositoryMap?.sha256 ?? null, locations };
  if (Buffer.byteLength(JSON.stringify(privateMapping)) > PROVENANCE_BOUNDS.exportBytes)
    throw new ImportError("private action provenance map exceeds its bound; use a smaller explicit input frame");
  return { body, privateMapping };
}
