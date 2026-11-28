/**
 * Slot 062360: the canonical digest of a session erasure replay manifest (design
 * docs/design/operator-erase-replay-target-manifest.md section 4). The byte-exact twin of
 * sessions.session_erasure_replay_digest(uuid); test vectors are pinned in
 * src/hoopdb/__tests__/fixtures/session-erasure-replay-digest-vectors.json and checked against both.
 *
 * Encoding, version 1:
 *   frame(s)  = 4-byte big-endian UTF-8 octet length of s, then those bytes
 *   count(n)  = frame(canonical base-10 text of n), no sign or leading zero (target counts are bigint range)
 *   message   = the raw ASCII bytes "rh-erase-replay-manifest/v1" (not framed), frame(capture_version, decimal: "1")
 *               frame(manifest_id, canonical lowercase UUID) frame(request_id) frame(org_id) frame(workspace_id)
 *               frame(source) frame(requested_session_id)
 *               count(|aliases|) frame(alias)...           aliases unique, bytewise ascending
 *               count(|targets|) (frame(user_id) frame(origin_id) frame(source) frame(session_id))...
 *                                                          targets unique, bytewise ascending on the 4-tuple
 *   digest    = lowercase hex SHA-256 of message
 *
 * Bytewise order is PostgreSQL COLLATE "C" on UTF-8 text: compare the UTF-8 bytes, never a locale.
 */
import { createHash } from "node:crypto";

export const SESSION_ERASURE_REPLAY_DIGEST_TAG = "rh-erase-replay-manifest/v1";

export interface ReplayTarget { user_id: string; origin_id: string; source: string; session_id: string }
export interface ReplayBinding {
  capture_version: 1;
  manifest_id: string;
  request_id: string;
  org_id: string;
  workspace_id: string;
  source: string;
  requested_session_id: string;
}

const utf8 = (s: string) => Buffer.from(s, "utf8");
function frame(s: string): Buffer {
  const b = utf8(s);
  const n = Buffer.alloc(4);
  n.writeUInt32BE(b.length);
  return Buffer.concat([n, b]);
}
function count(n: number): Buffer {
  if (!Number.isSafeInteger(n) || n < 0) throw Error("replay digest: count out of range");
  return frame(String(n));
}
/** COLLATE "C" on UTF-8: bytewise. */
export function compareC(a: string, b: string): number {
  return Buffer.compare(utf8(a), utf8(b));
}
export function compareTargets(a: ReplayTarget, b: ReplayTarget): number {
  return compareC(a.user_id, b.user_id) || compareC(a.origin_id, b.origin_id) || compareC(a.source, b.source) || compareC(a.session_id, b.session_id);
}

/** A streaming digest: feed the binding, the aliases, then targets in order (as a page drain does). */
export class ReplayDigest {
  private readonly h = createHash("sha256");
  private last: ReplayTarget | null = null;
  private seen = 0;
  constructor(binding: ReplayBinding, aliases: readonly string[], private readonly targetCount: number) {
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u.test(binding.manifest_id)) throw Error("replay digest: manifest_id is not a canonical uuid");
    for (let i = 1; i < aliases.length; i++) if (compareC(aliases[i - 1]!, aliases[i]!) >= 0) throw Error("replay digest: aliases not unique and ascending");
    if (binding.capture_version !== 1) throw Error("replay digest: unsupported capture version");
    this.h.update(Buffer.from(SESSION_ERASURE_REPLAY_DIGEST_TAG, "ascii"));
    this.h.update(frame(String(binding.capture_version)));
    for (const v of [binding.manifest_id, binding.request_id, binding.org_id, binding.workspace_id, binding.source, binding.requested_session_id]) this.h.update(frame(v));
    this.h.update(count(aliases.length));
    for (const a of aliases) this.h.update(frame(a));
    this.h.update(count(targetCount));
  }
  push(t: ReplayTarget): void {
    if (this.last && compareTargets(this.last, t) >= 0) throw Error("replay digest: targets not unique and ascending");
    for (const v of [t.user_id, t.origin_id, t.source, t.session_id]) this.h.update(frame(v));
    this.last = t;
    this.seen++;
  }
  hex(): string {
    if (this.seen !== this.targetCount) throw Error("replay digest: target count mismatch");
    return this.h.digest("hex");
  }
}

/** The digest of a complete manifest; aliases and targets are sorted here. */
export function replayManifestDigest(binding: ReplayBinding, aliases: readonly string[], targets: readonly ReplayTarget[]): string {
  const d = new ReplayDigest(binding, [...aliases].sort(compareC), targets.length);
  for (const t of [...targets].sort(compareTargets)) d.push(t);
  return d.hex();
}
