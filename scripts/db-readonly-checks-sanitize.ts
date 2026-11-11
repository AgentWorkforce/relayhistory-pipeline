/**
 * Artifact sanitizer for scripts/db-readonly-checks.ts. SQL-free and side-effect-free.
 *
 * The artifact is meant to hold only booleans, counts, timestamps, catalog names and planner estimates.
 * This walks every key and string value and refuses (throws) on anything shaped like an identifier or a
 * credential: UUIDs, 32+ hex runs, JWTs, URLs, e-mail addresses, Supabase hostnames, 20-character
 * project-ref-shaped tokens, and any caller-supplied secret fragment (password, host, user, project ref).
 * A refusal fails the run and no artifact is written.
 */

export class SanitizeError extends Error {
  override name = "SanitizeError";
  constructor(readonly code: string, readonly path: string) {
    super(`artifact refused: ${code} at ${path}`);
  }
}

export const REFUSE_PATTERNS: readonly { code: string; re: RegExp }[] = [
  { code: "uuid", re: /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i },
  { code: "hex32", re: /[0-9a-f]{32}/i }, // also an undashed UUID
  { code: "jwt", re: /eyJ[A-Za-z0-9_-]{2,}\.[A-Za-z0-9_-]{2,}/ },
  { code: "url", re: /[a-z][a-z0-9+.-]{1,20}:\/\//i },
  { code: "email", re: /[^\s@'"]+@[^\s@'"]+\.[a-z]{2,}/i },
  { code: "supabase_host", re: /supabase\.(co|com|net|in)\b|pooler\./i },
  // A 20-character lower-case alphanumeric token with at least one letter (Supabase project-ref shape).
  { code: "project_ref_shape", re: /(?<![a-z0-9])(?=[a-z0-9]{0,19}[a-z])[a-z0-9]{20}(?![a-z0-9])/ },
];

const MAX_STRING = 256;

/** Throws SanitizeError on the first refused value; returns the input unchanged otherwise. */
export function sanitizeArtifact<T>(value: T, secrets: readonly string[] = []): T {
  const fragments = secrets.filter((s) => typeof s === "string" && s.length >= 4);
  const checkString = (s: string, path: string) => {
    if (s.length > MAX_STRING) throw new SanitizeError("string_too_long", path);
    for (const { code, re } of REFUSE_PATTERNS) if (re.test(s)) throw new SanitizeError(code, path);
    for (const f of fragments) if (s.includes(f)) throw new SanitizeError("secret_fragment", path);
  };
  const walk = (v: unknown, path: string, depth: number): void => {
    if (depth > 12) throw new SanitizeError("too_deep", path);
    if (v === null || typeof v === "boolean") return;
    if (typeof v === "number") {
      if (!Number.isFinite(v)) throw new SanitizeError("non_finite", path);
      return;
    }
    if (typeof v === "string") return checkString(v, path);
    if (Array.isArray(v)) {
      v.forEach((x, i) => walk(x, `${path}[${i}]`, depth + 1));
      return;
    }
    if (typeof v === "object" && Object.getPrototypeOf(v) === Object.prototype) {
      for (const [k, x] of Object.entries(v as Record<string, unknown>)) {
        checkString(k, `${path}.<key>`);
        walk(x, `${path}.${k}`, depth + 1);
      }
      return;
    }
    throw new SanitizeError("unsupported_type", path);
  };
  walk(value, "$", 0);
  return value;
}
