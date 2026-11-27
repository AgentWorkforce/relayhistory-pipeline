/**
 * The one parser of GitHub repository names for relayfacts: a remote or slug becomes `owner/name`, or null.
 * Accepted, with the host exactly `github.com` (any case) and nothing after the repository but `.git` or `/`:
 *   owner/name                       github.com/owner/name
 *   https://github.com/owner/name    http(s)://<user>@github.com/owner/name
 *   ssh://git@github.com/owner/name  git@github.com:owner/name
 * Anything else abstains: another host (a mirror whose path merely contains `github.com`), a port, a query or
 * fragment anywhere (also inside what would be userinfo: `https://mirror.invalid?@github.com/...` has host
 * mirror.invalid), a backslash or whitespace, extra path segments, a scrubbed value. A URL form is accepted only
 * when the WHATWG parser agrees on every part of the authority: host exactly github.com, no port, no query or
 * fragment, and the path the pattern matched.
 */
const PART = "[A-Za-z0-9_.-]+";
/** RFC 3986 userinfo without the characters that end an authority early (`/ ? # @`) or that parsers disagree on. */
const USERINFO = "[A-Za-z0-9._~%!$&'()*+,;=-]+(?::[A-Za-z0-9._~%!$&'()*+,;=-]*)?";

/** The WHATWG parser's view of a URL form must be exactly github.com, no port, query, fragment or extra path. */
function authorityAgrees(v: string, owner: string, name: string): boolean {
  if (!/^(?:https?|ssh):\/\//iu.test(v)) return true;
  let u: URL;
  try { u = new URL(v); } catch { return false; }
  const path = u.pathname.replace(/\/$/u, "").replace(/\.git$/iu, "");
  return u.hostname.toLowerCase() === "github.com" && u.port === "" && u.search === "" && u.hash === "" && path.toLowerCase() === `/${owner}/${name}`.toLowerCase();
}
const FORMS = [
  new RegExp(`^(${PART})/(${PART}?)(?:\\.git)?/?$`, "u"),
  new RegExp(`^github\\.com/(${PART})/(${PART}?)(?:\\.git)?/?$`, "iu"),
  new RegExp(`^https?://(?:${USERINFO}@)?github\\.com/(${PART})/(${PART}?)(?:\\.git)?/?$`, "iu"),
  new RegExp(`^ssh://(?:${USERINFO}@)?github\\.com/(${PART})/(${PART}?)(?:\\.git)?/?$`, "iu"),
  new RegExp(`^[A-Za-z0-9_.-]+@github\\.com:(${PART})/(${PART}?)(?:\\.git)?/?$`, "iu"),
];

export function githubSlug(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const v = value.trim();
  if (!v || v.length > 300 || /REDACTED/u.test(v) || /[?#\\\s]/u.test(v)) return null;
  for (const re of FORMS) {
    const m = re.exec(v);
    if (!m) continue;
    const owner = m[1]!, name = m[2]!;
    if ([owner, name].some((p) => p === "." || p === ".." || !p)) return null;
    if (!authorityAgrees(v, owner, name)) return null;
    return `${owner}/${name}`.toLowerCase();
  }
  return null;
}
