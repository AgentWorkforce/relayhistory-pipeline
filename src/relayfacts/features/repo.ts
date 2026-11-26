/**
 * The one parser of GitHub repository names for relayfacts: a remote or slug becomes `owner/name`, or null.
 * Accepted, with the host exactly `github.com` (any case) and nothing after the repository but `.git` or `/`:
 *   owner/name                       github.com/owner/name
 *   https://github.com/owner/name    http(s)://<user>@github.com/owner/name
 *   ssh://git@github.com/owner/name  git@github.com:owner/name
 * Anything else abstains: another host (a mirror whose path merely contains `github.com`), a port, a query or
 * fragment, extra path segments, a scrubbed value.
 */
const PART = "[A-Za-z0-9_.-]+";
const FORMS = [
  new RegExp(`^(${PART})/(${PART}?)(?:\\.git)?/?$`, "u"),
  new RegExp(`^github\\.com/(${PART})/(${PART}?)(?:\\.git)?/?$`, "iu"),
  new RegExp(`^https?://(?:[^@/\\s]+@)?github\\.com/(${PART})/(${PART}?)(?:\\.git)?/?$`, "iu"),
  new RegExp(`^ssh://(?:[^@/\\s]+@)?github\\.com/(${PART})/(${PART}?)(?:\\.git)?/?$`, "iu"),
  new RegExp(`^[A-Za-z0-9_.-]+@github\\.com:(${PART})/(${PART}?)(?:\\.git)?/?$`, "iu"),
];

export function githubSlug(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const v = value.trim();
  if (!v || v.length > 300 || /REDACTED/u.test(v)) return null;
  for (const re of FORMS) {
    const m = re.exec(v);
    if (!m) continue;
    const owner = m[1]!, name = m[2]!;
    if ([owner, name].some((p) => p === "." || p === ".." || !p)) return null;
    return `${owner}/${name}`.toLowerCase();
  }
  return null;
}
