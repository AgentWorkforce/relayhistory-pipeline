const REDACTED = "[REDACTED]";

type Replacement =
  | string
  | ((substring: string, ...groups: string[]) => string);

const isWordChar = (char: string | undefined) =>
  !!char && /[A-Za-z0-9_]/.test(char);

/**
 * Redacts every `header.payload.signature` JWT inside one dot-joined token run: the
 * header starts with `eyJ` at a word boundary (8+ chars after it), the payload is a
 * whole segment starting `eyJ` (8+ chars after it), and the signature a whole segment
 * of 16+ chars.
 */
function redactJwtsInRun(run: string): string {
  if (!run.includes(".eyJ")) return run;
  const segments = run.split(".");
  const out: string[] = [];
  for (let i = 0; i < segments.length; i += 1) {
    const header = segments[i] as string;
    const payload = segments[i + 1];
    const signature = segments[i + 2];
    if (
      payload &&
      signature &&
      payload.startsWith("eyJ") &&
      payload.length >= 11 &&
      signature.length >= 16
    ) {
      let start = header.indexOf("eyJ");
      while (
        start !== -1 &&
        (isWordChar(header[start - 1]) || header.length - start < 11)
      ) {
        start = isWordChar(header[start - 1])
          ? header.indexOf("eyJ", start + 1)
          : -1;
      }
      if (start !== -1) {
        out.push(header.slice(0, start) + REDACTED);
        i += 2;
        continue;
      }
    }
    out.push(header);
  }
  return out.join(".");
}

/**
 * Whether a key names a secret, for the quoted key/value rule. Compound names
 * (`api_key`, `accessKey`, `SecretAccessKey`, `client-secret`, `sessiontoken`…) count
 * wherever they end the key. Single words (`password`, `token`, `auth`…) count only as a
 * whole word at the end of the key: at its start, after a non-letter (`db_password`,
 * `x-auth-token`, `cf.token`) or at a camelCase hump (`githubToken`), so `oauth`,
 * `author` and `tokens` are not secret names. A bare `key` is not one either: quoted
 * `"key"` is everywhere in tool output (S3 object keys, keyboard events, cache keys).
 */
const SECRET_KEY_COMPOUND =
  /(?:api|access|private|secret|auth|signing|encryption|master)[_ -]?key$|client[_ -]?secret$|(?:session|access|refresh|auth|id|bot|bearer|api)[_ -]?token$/i;
const SECRET_KEY_WORD =
  /(?:password|passwd|passphrase|secret|token|auth|authorization|credentials?|bearer)$/i;

export function isSecretKeyName(name: string): boolean {
  if (SECRET_KEY_COMPOUND.test(name)) return true;
  const m = SECRET_KEY_WORD.exec(name);
  if (!m) return false;
  if (m.index === 0) return true;
  const prev = name[m.index - 1] as string;
  if (!/[A-Za-z]/.test(prev)) return true;
  return /[a-z]/.test(prev) && /[A-Z]/.test(name[m.index] as string);
}

/**
 * A quoted key at any JSON escaping depth (`"k"`, `\"k\"`, `\\\"k\\\"`, `'k'`, `` `k` ``),
 * then `:`, `=` or `=>` with optional whitespace. The leading lookbehind starts the
 * match at the beginning of a backslash run, and every quantifier is bounded, so each
 * start position is O(1) and the scan is linear.
 */
const QUOTED_KEY =
  /(?<!\\)(\\{0,16})(["'`])([A-Za-z0-9_. -]{1,64})\1\2([ \t]{0,16}(?:=>|[:=])[ \t]{0,16})/g;

/**
 * How far the quoted key/value rule looks for a quoted value's matching closing quote,
 * in UTF-16 units from the value's start and across line breaks, before it treats the
 * value as over-long. A scan bound, not a storage bound (rule set 4): a value whose
 * matching close is not within it is redacted whole, up to that close or, with none, to
 * the end of the input (line breaks do not end it), so no part of it is stored. From rule
 * set 5 every quoted value is redacted that way; the bound only decides which values the
 * first pass (`redactOverCapQuotedValues`) takes and which are over-cap spans.
 */
export const MAX_QUOTED_VALUE = 4096;

/**
 * An unquoted value after a quoted key: up to whitespace (a line break included), a quote,
 * a backslash or a delimiter (`,;{}[]`). Sticky, matched in place from the value's start.
 */
const BARE_VALUE = /[^\s"'`,;{}[\]\\]*/y;

/** The run of non-whitespace from a position (rule set 6: the rest of a token glued to a private-key footer). */
const GLUED_RUN = /\S*/y;

/**
 * JSON literals and numbers after a quoted secret key are not secrets (`"max_token": 512`).
 * Tested only on a whole unquoted value within the bound (rule set 6).
 */
const NON_SECRET_BARE =
  /^(?:true|false|null|undefined|none|-?\d+(?:\.\d+)?(?:e[+-]?\d+)?)$/i;

/** A line separator: CR, LF (so CRLF too), U+2028, U+2029 or U+0085. */
const isLineBreak = (c: string | undefined) =>
  c === "\n" || c === "\r" || c === "\u2028" || c === "\u2029" || c === "\u0085";

/**
 * A complete one-line 1Password reference, the whole value: `op://`, then exactly three or
 * four `/`-separated segments (`vault/item/field` or `vault/item/section/field`; two, or
 * five or more, are no reference and no query string is allowed), each one or more
 * runs of ASCII letters, digits, `.`, `_` or `-` joined by single spaces. No backslash,
 * quote, control character, line separator, escape sequence (`\n`, `\u2028`, …), tab,
 * double space, or leading or trailing space in a segment.
 */
export const OP_REFERENCE =
  /^op:\/\/[A-Za-z0-9._-]+(?: [A-Za-z0-9._-]+)*(?:\/[A-Za-z0-9._-]+(?: [A-Za-z0-9._-]+)*){2,3}$/;

/**
 * Values the quoted key/value rule keeps: already redacted, or a 1Password reference
 * (`OP_REFERENCE`, the whole value). Rule set 5 keeps nothing else that starts with
 * `op://`: such a value is redacted whole. A value that held a line separator as an
 * escape sequence (a backslash then `n`, `r`, `u2028`, … in nested or escaped JSON) was
 * kept as a one-line reference before, tail included; a backslash is never part of one.
 */
const keepQuotedValue = (value: string) => value === REDACTED || OP_REFERENCE.test(value);

/** Number of backslashes immediately before `at`. */
function backslashesBefore(text: string, at: number): number {
  let n = 0;
  while (at - n - 1 >= 0 && text[at - n - 1] === "\\") n += 1;
  return n;
}

/**
 * Redact the value of every quoted secret key, keeping the key, the separator and the
 * value's quotes: `"password": "x"`, `\"password\": \"x\"` (a JSON document inside a
 * JSON string, such as a tool result), deeper escapings, single-quoted and backtick
 * forms, and a generated-looking unquoted value after a quoted one-word key
 * (`'token' => <generated>`). A quoted value ends at the matching closing quote: a
 * quote escaped once more than the opening one (an inner quote) does not end it, and
 * nor does one preceded by an escaped backslash. JSON literals and numbers are kept, as
 * are values already redacted, complete one-line `op://` references (`OP_REFERENCE`, the
 * whole value; any other value starting `op://` is redacted whole), and an empty value.
 *
 * Rule set 5: every quoted value is redacted whole, whatever its length: through its
 * matching closing quote, across every line break (CR, LF, CRLF, U+2028, U+2029,
 * U+0085), or, with no matching close, to the end of the input (fail closed). Rule set 4
 * redacted a value within the bound only to its close on the key's line, else to the
 * next whitespace, so `"password": "line1<LF>tail"` stored `tail`.
 *
 * Rule set 4 caps the value's total length, not its line's: the value is over the bound
 * when its matching closing quote (the same test) is not within `MAX_QUOTED_VALUE`
 * UTF-16 units of its start, counted across line breaks (CR, LF, CRLF, U+2028, U+2029,
 * U+0085) and the input continues past them. Such a value is redacted whole, whatever
 * it holds: up to its matching closing quote, or to the end of the input when there is
 * none. No line break ends it: a template literal, an escaped line continuation or a
 * malformed value can span lines, and a tail left after a break would have no key for a
 * later rule to find. A value whose matching close is within the bound, or whose input
 * ends within it, is redacted the same way from rule set 5 (rule set 4 redacted it as
 * rule set 3 did). An unquoted value is classified whole, to its real end (rule set 6;
 * rule set 5 classified its first `MAX_QUOTED_VALUE` units, so 4096 digits then a tail
 * passed as a number and the tail was kept): one of `MAX_QUOTED_VALUE` units or more is
 * redacted whole, whatever it holds; a shorter one is kept when it is a JSON literal, a
 * number or not generated-looking.
 *
 * Linear: one forward walk per value from its start finds the close, whether a line
 * break precedes it, and the bound together; the walk ends at the next quote of the
 * value's own kind (quote and backslash depth), which every later value of that kind opens
 * with, so the walks of one kind never overlap; past the bound the walk goes on to the
 * close or the end, and a value with no close runs to the end; the redaction consumes
 * what was walked. With `overCap`, the span [key start, value end) of
 * each over-long quoted value and of each generated unquoted value of `MAX_QUOTED_VALUE`
 * units or more is recorded; with `multiLine` too, so is that of each quoted value within
 * the bound that holds a line break (to its close, or the end of the input). With
 * `prefix` (the text is a prefix of a longer input; for spans only, the output is not
 * used), a quoted value with no matching close in the text may run on past it in the full
 * input, so it is recorded open, to the end of the text.
 */
function redactQuotedSecretValues(
  text: string,
  overCap?: Array<[number, number]>,
  prefix = false,
  onlyOverCap = false,
  multiLine = false,
): string {
  if (!/[:=]/.test(text)) return text;
  let out = "";
  let last = 0;
  // First pass only: the private-key pairing over the text kept so far (see below).
  let pemFrom = 0;
  let pem = { open: false, at: -1, closedOuter: false };
  QUOTED_KEY.lastIndex = 0;
  for (let m = QUOTED_KEY.exec(text); m; m = QUOTED_KEY.exec(text)) {
    const end = m.index + m[0].length;
    if (!isSecretKeyName((m[3] as string).trim())) continue;
    let bs = 0;
    while (bs < 16 && text[end + bs] === "\\") bs += 1;
    const quote = text[end + bs];
    let valueStart: number;
    let valueEnd: number;
    // Past the bound: redacted whole, never kept.
    let longValue = false;
    // A generated unquoted value of `MAX_QUOTED_VALUE` units or more: redacted as
    // before (to its end), and its span recorded for the segmenter.
    let overBound = false;
    // A quoted value within the bound that holds a line break: its span is recorded
    // with `multiLine` (the segmenter must not cut inside it either).
    let lineValue = false;
    if (quote === '"' || quote === "'" || quote === "`") {
      valueStart = end + bs + 1;
      // At escaping depth d the opening quote has 2^d - 1 backslashes, and a quote is
      // the closing one when its backslash run is that many modulo 2^(d+1): one more
      // backslash pair is an escaped backslash, an odd excess is an inner quote.
      const period = (bs + 1) & bs ? 0 : 2 * (bs + 1);
      const closesAt = (i: number) => {
        const run = backslashesBefore(text, i);
        const own = i - run >= valueStart ? run : i - valueStart;
        return period ? own % period === bs : own === bs;
      };
      /** From `i`: the value's end at its matching closing quote, or the end of the input (no line break ends it). */
      const restOfValue = (i: number) => {
        for (; i < text.length; i += 1) {
          if (text[i] === quote && closesAt(i)) return i - bs;
        }
        return text.length;
      };
      // One forward walk, counting UTF-16 units across line breaks: the matching close,
      // whether a line break comes before it, and whether the close is within the bound
      // (its quote at most MAX_QUOTED_VALUE units past the value's start, as in rule set
      // 3's scan; for an escaped quote its backslashes count as value units).
      const limit = Math.min(text.length, valueStart + MAX_QUOTED_VALUE + 1);
      let close = -1;
      let i = valueStart;
      for (; i < limit; i += 1) {
        const c = text[i];
        if (!lineValue && isLineBreak(c)) lineValue = true;
        if (c === quote && closesAt(i)) {
          close = i;
          break;
        }
      }
      if (close === -1 && (i - valueStart > MAX_QUOTED_VALUE || prefix)) {
        // No matching close within the bound and the input goes on past it (or, for a
        // prefix, may): the whole value, across lines, to its close or the end.
        longValue = true;
        lineValue = false;
        valueEnd = restOfValue(i);
      } else {
        // Within the bound (rule set 5): the whole value, across lines, to its matching
        // close or, with none, to the end of the input. No line break ends it and there
        // is no whitespace fallback: a tail left after a break would have no key before it.
        valueEnd = close === -1 ? text.length : close - bs;
      }
    } else {
      // An unquoted value is taken only after a one-word key (no space: in
      // `Error 401 "Invalid authentication token": please run …` the quotes hold
      // prose) and only when it looks generated (`looksGenerated`), so code is kept:
      // `case "token": return parseToken(input);`, `case 'auth': handleAuth(req)`.
      // A short or word-like unquoted password after a quoted key is kept too.
      if (bs > 0 || (m[3] as string).includes(" ")) continue;
      valueStart = end;
      // Rule set 6: the whole value, to its real end, before any decision. Rule set 5
      // classified only its first MAX_QUOTED_VALUE units, so 4096 digits then a tail
      // passed as a number (or a low-entropy prefix) and the tail was stored.
      BARE_VALUE.lastIndex = end;
      const bare = (BARE_VALUE.exec(text) as RegExpExecArray)[0];
      if (!bare) continue;
      valueEnd = end + bare.length;
      // Past the bound: redacted whole, whatever it holds (no literal, number or entropy test).
      overBound = bare.length >= MAX_QUOTED_VALUE;
      if (!overBound && (NON_SECRET_BARE.test(bare) || !looksGenerated(bare))) continue;
    }
    const value = longValue ? "" : text.slice(valueStart, valueEnd);
    // A prefix may end right at an open value: still recorded (open), with nothing to redact.
    if (longValue && valueEnd <= valueStart) {
      overCap?.push([m.index, valueEnd]);
      continue;
    }
    if (valueEnd <= valueStart || (!longValue && keepQuotedValue(value))) continue;
    // From the key start: a cut between key and value would leave the value with no key to match.
    if (longValue || overBound || (multiLine && lineValue)) overCap?.push([m.index, valueEnd]);
    QUOTED_KEY.lastIndex = Math.max(QUOTED_KEY.lastIndex, valueEnd);
    if (!onlyOverCap) {
      out += text.slice(last, valueStart) + REDACTED;
      last = valueEnd;
      continue;
    }
    // Rule set 5's first pass: only over-bound values are redacted, but every value the
    // full pass would redact is still skipped, so both passes see the same keys. Rule set
    // 6: so is a value of any length that holds a private-key boundary the PEM rule would
    // pair across the value's edge, as below.
    //
    // The PEM rule runs on this pass's output and pairs each header with the next footer,
    // left to right (`pairPrivateKeys`), so redacting a value must leave it no header or
    // footer whose partner was inside the value. `pem` is that pairing over the text this
    // pass keeps, up to the value's start.
    pem = pairPrivateKeys(text.slice(pemFrom, valueStart), pem.open, pem.at, out.length + pemFrom - last);
    pemFrom = valueStart;
    const inside = pairPrivateKeys(text.slice(valueStart, valueEnd), pem.open, -1, 0);
    // A header before the value whose footer is inside it (rule set 5 took the footer with
    // the value and the PEM rule then redacted from the header to the end of the input):
    // the redaction starts at that header and runs through the value's closing quote.
    const fromHeader = inside.closedOuter;
    // A header inside the value whose footer is after it: the redaction runs to that footer
    // (or the end), as the PEM rule's would.
    const toFooter = inside.at !== -1;
    if (!longValue && !overBound && !fromHeader && !toFooter) continue;
    // The value's closing quote (its backslashes and the quote), when it has one.
    const closeLength = quote === '"' || quote === "'" || quote === "`" ? bs + 1 : 0;
    const closeEnd = Math.min(text.length, valueEnd + closeLength);
    let redactEnd = valueEnd;
    // Whether the closing quote taken with a redaction to a footer is put back (below).
    let restoreClose = !fromHeader;
    if (toFooter) {
      PEM_END.lastIndex = closeEnd;
      const footer = PEM_END.exec(text);
      redactEnd = footer ? footer.index + footer[0].length : text.length;
      // A footer after a quote may be inside a later quoted value (`"token": "...-----END
      // ...-----SECRET"`), whose key this redaction removes: the rest of that value would
      // be kept with no key in view. Then, as rule set 5 did, the close is not put back, so
      // the quoted rule takes what follows the footer as part of this still-open value;
      // when this value's key goes too (a header before it), nothing would take that rest,
      // so the redaction runs to the end of the input, as rule set 5's did.
      if (footer && /["'`]/.test(text.slice(closeEnd, footer.index))) {
        restoreClose = false;
        if (fromHeader) redactEnd = text.length;
      } else if (redactEnd < text.length) {
        // With no quote between, the footer may be inside a later UNQUOTED credential
        // (`token=…`, `Bearer …`, `?access_token=…`, a bare token) whose label this
        // redaction removes: the rest of that value, glued to the footer, would be kept
        // with no label for its own rule to find. So the redaction also takes the run of
        // non-whitespace glued to the footer, up to a private-key header that starts in it
        // (that block is the PEM rule's). A quote in that run may open or close a value
        // whose extent cannot be told here: then, as rule set 5's did, the redaction runs
        // to the end of the input.
        GLUED_RUN.lastIndex = redactEnd;
        let glued = (GLUED_RUN.exec(text) as RegExpExecArray)[0];
        // A header holds a space, so one can start in the run only at its very end.
        if (glued.endsWith("-----BEGIN")) {
          PEM_BEGIN_AT.lastIndex = redactEnd + glued.length - 10;
          if (PEM_BEGIN_AT.test(text)) glued = glued.slice(0, -10);
        }
        redactEnd = /["'`]/.test(glued) ? text.length : redactEnd + glued.length;
      }
    } else if (fromHeader) redactEnd = closeEnd;
    out += text.slice(last, valueStart);
    if (fromHeader) out = out.slice(0, pem.at);
    out += REDACTED;
    // The key and opening quote are kept and the closing quote was taken: put it back, or
    // the quoted rule would find the value unclosed and redact the rest of the input.
    if (restoreClose && redactEnd > valueEnd) out += text.slice(valueEnd, closeEnd);
    last = redactEnd;
    pemFrom = redactEnd;
    // After a footer, nothing is open; otherwise a header before the value stays open.
    pem = { open: !toFooter && inside.open, at: pem.at, closedOuter: false };
    QUOTED_KEY.lastIndex = Math.max(QUOTED_KEY.lastIndex, redactEnd);
  }
  return last === 0 ? text : out + text.slice(last);
}

/**
 * Pair private-key headers and footers in `s` left to right, as the PEM rule does (each
 * header with the next footer after it), starting `open` when a header before `s` (at
 * `at` in the caller's output) is still waiting for its footer. Returns whether a header
 * is open after `s` and where it starts (`base` + its index in `s`, or `at` when it is the
 * header from before `s`; -1 when that one is from before and `at` was -1), and whether a
 * footer in `s` closed the header that was open before it.
 */
function pairPrivateKeys(
  s: string,
  open: boolean,
  at: number,
  base: number,
): { open: boolean; at: number; closedOuter: boolean } {
  let closedOuter = false;
  let outer = open;
  let pos = 0;
  for (;;) {
    const re = open ? PEM_END : PEM_BEGIN;
    re.lastIndex = pos;
    const m = re.exec(s);
    if (!m) return { open, at: open ? at : -1, closedOuter };
    pos = m.index + m[0].length;
    if (open) {
      if (outer) closedOuter = true;
      outer = false;
      open = false;
    } else {
      open = true;
      at = base + m.index;
    }
  }
}

/**
 * The span [key start, value end) of every quoted secret value in `text` whose matching
 * closing quote is not within the quoted rule's scan bound (counted across line breaks),
 * and of every generated unquoted value after a quoted key of at least that length, as
 * `redactQuotedSecretValues` finds them. The rule redacts each whole, so a segmenter must
 * not cut inside one: the segment after the cut would hold the rest of the value with no
 * key before it. Whether a value is over the bound is decided by the first
 * `MAX_QUOTED_VALUE` + 150 units from its key's start (a key match is at most 132 units
 * and up to 16 backslashes and the quote follow it, then `MAX_QUOTED_VALUE` + 1 units of
 * value, across lines).
 *
 * With `prefix: true` the text is taken as a prefix of a longer input: a quoted value
 * the text ends inside before that decision (no matching close in what is seen) is
 * reported as possibly over the bound, open to the end of the text (fail closed). A
 * caller that passes its whole input leaves `prefix` false and gets exact spans.
 */
export function overCapQuotedValueSpans(
  text: string,
  options: { prefix?: boolean } = {},
): Array<[number, number]> {
  const spans: Array<[number, number]> = [];
  redactQuotedSecretValues(text, spans, options.prefix ?? false);
  return spans;
}

/**
 * Every span `overCapQuotedValueSpans` reports, and also the span [key start, value end)
 * of every quoted secret value within the bound that holds a line break (CR, LF, CRLF,
 * U+2028, U+2029, U+0085), through its matching close or, with none, to the end of the
 * text (rule set 5). The quoted rule redacts each of these whole too, across its line
 * breaks, so a segmenter must not cut inside one either: the segment after the cut would
 * hold the value's later lines with no key before them. Rule set 9 adds the span [name
 * start, closing quote end) of every quoted secret assignment value holding whitespace
 * (`redactSecretAssignments` redacts it whole). Spans are sorted and disjoint.
 * `prefix: true` is as for `overCapQuotedValueSpans`: a value with no matching close in
 * the text is reported open to the end of the text (fail closed).
 */
export function unsplittableQuotedValueSpans(
  text: string,
  options: { prefix?: boolean } = {},
): Array<[number, number]> {
  const spans: Array<[number, number]> = [];
  redactQuotedSecretValues(text, spans, options.prefix ?? false, false, true);
  // Rule set 9: a quoted assignment value holding whitespace is redacted whole too.
  redactSecretAssignments(text, spans, options.prefix ?? false);
  spans.sort((a, b) => a[0] - b[0] || b[1] - a[1]);
  const merged: Array<[number, number]> = [];
  for (const [start, end] of spans) {
    const previous = merged[merged.length - 1];
    if (previous && start < previous[1]) previous[1] = Math.max(previous[1], end);
    else merged.push([start, end]);
  }
  return merged;
}

/**
 * Rule set 5's first rule: every quoted secret value over the scan bound, and every
 * generated unquoted value of `MAX_QUOTED_VALUE` units or more after a quoted key (the
 * values `overCapQuotedValueSpans` reports), redacted whole on the text as given, before
 * any other rule. Rule set 4 measured a value only after the token rules had run, and they
 * can shrink it (each provider-shaped token becomes the shorter marker): a multi-line value
 * over the bound in the input could fall under it, be taken for an ordinary value, and keep
 * everything after its first line. Values within the bound are left to the quoted rule in
 * its usual place (which, from rule set 5, also redacts them whole across lines).
 *
 * Rule set 6: the private-key rule runs next, on this rule's output, so this rule keeps
 * its header/footer pairing intact. A value (of any length) holding the footer of a
 * header before it is redacted from that header through the value's closing quote; one
 * holding a header whose footer is after it, through that footer. Rule set 5 took the
 * footer with the value, and the private-key rule then redacted from the header to the
 * end of the input. When a quote lies between the value and that later footer, the footer
 * may be inside another quoted value whose key the redaction removes, so the rest of that
 * value is never kept: the closing quote is not put back (the quoted rule then takes the
 * rest as part of this value), or, when the redaction also starts at a header before the
 * value, it runs to the end of the input. With no quote between, the footer may be inside
 * a later unquoted credential (`token=…`, `Bearer …`, `?access_token=…`) whose label the
 * redaction removes: the run of non-whitespace glued to the footer is redacted with it
 * (to the end of the input when that run holds a quote), so none of it is kept unlabelled.
 */
function redactOverCapQuotedValues(text: string): string {
  return redactQuotedSecretValues(text, undefined, false, true);
}

/** The start of every literal-prefix rule (see the provider key prefixes). */
const TOKEN_START = "(?<![A-Za-z0-9_-])";

/**
 * Each literal-prefix rule (those starting with `TOKEN_START`) for a prefix glued to a
 * longer token by `-` (`word-sk-…`, `foo-ghp_…`, `id-glpat-…`), which the old `\b` start
 * caught. The whole `-`-joined token is redacted, not only the prefixed part, so neither
 * the start of a key it is glued to nor a second glued key after it is left behind. Run
 * after every full-shape rule (SendGrid, GitLab, Slack, Capy, relay, JWT, Google), so a
 * key whose body holds `-sk-…` is already redacted whole. A match starts only where a
 * token starts and tries the literal prefix at each `-` in it, so it costs no more than
 * the old `\b` start did. The relay_ rule goes first: its body may hold `.`, which no other
 * prefix rule's does, so a `sk-` rule run first would stop at the `.` and split a glued
 * `relay_…-sk-….tail` token. Built on first use from `SECRET_PATTERNS`, so the two lists
 * cannot drift apart.
 */
let gluedPrefixRules: [RegExp, Replacement][] | undefined;
function redactGluedPrefixes(text: string): string {
  if (!gluedPrefixRules) {
    const rules = SECRET_PATTERNS.flatMap((rule): [RegExp, Replacement][] =>
      typeof rule !== "function" && rule[0].source.startsWith(TOKEN_START)
        ? [
            [
              new RegExp(
                `${TOKEN_START}[A-Za-z0-9_-]*?-(?:${rule[0].source.slice(TOKEN_START.length)})[A-Za-z0-9_-]*`,
                rule[0].flags,
              ),
              rule[1],
            ],
          ]
        : [],
    );
    const isRelay = ([pattern]: [RegExp, Replacement]) => pattern.source.includes("relay_");
    gluedPrefixRules = [...rules.filter(isRelay), ...rules.filter((r) => !isRelay(r))];
  }
  return gluedPrefixRules.reduce(
    (out, [pattern, replacement]) => out.replace(pattern, replacement as any),
    text,
  );
}

/** Shannon entropy of `s` in bits per character. */
function entropy(s: string): number {
  const counts = new Map<string, number>();
  for (const c of s) counts.set(c, (counts.get(c) ?? 0) + 1);
  let h = 0;
  for (const n of counts.values()) {
    const p = n / s.length;
    h -= p * Math.log2(p);
  }
  return h;
}

/**
 * Whether a token next to a secret name looks generated: 20+ characters drawing on at
 * least two of lowercase, uppercase and digits, with at least 3.5 bits of entropy per
 * character. Only ever asked about a token directly after a secret name (the fallback
 * rule), never about free text, so commit shas, digests and ids elsewhere in a
 * transcript are kept.
 */
function looksGenerated(token: string): boolean {
  const classes =
    Number(/[a-z]/.test(token)) +
    Number(/[A-Z]/.test(token)) +
    Number(/\d/.test(token));
  return token.length >= 20 && classes >= 2 && entropy(token) >= 3.5;
}

type Rule = [RegExp, Replacement] | ((text: string) => string);

/**
 * `<name>=<value>` / `<name>: <value>` secret assignments. The identifier may be prefixed
 * (STRIPE_KEY, AWS_SECRET_ACCESS_KEY, GITHUB_TOKEN, DB_PASSWORD) — `(?:[A-Za-z0-9]+_){0,8}`
 * absorbs the prefix segments so the trailing sensitive word still anchors. The leading `\b`
 * + required underscore segments means a bare word like `monkey=…` is NOT matched.
 *
 * `(?:[A-Za-z0-9]+_){0,8}` is bounded, not `*`: an unbounded prefix class in front of a
 * literal is the same quadratic shape fixed in the URL and email patterns. Eight
 * underscore-separated segments is far past any real identifier (`AWS_SECRET_ACCESS_KEY`
 * is three).
 *
 * Only the public identifier is skipped as a match, and scanning goes on into its value:
 * consuming an exempt assignment would hide a nested `password=...` from this rule (rule
 * set 8).
 *
 * Rule set 9: the separator is a literal `=` or `:`, or one of them encoded as tool output
 * echoes it: a unicode or hex escape written out (`\u003d`, `\u003a`, `\x3d`, `\x3a`,
 * behind 1 to 16 backslashes, so at any JSON escaping depth up to 4) or percent-encoded
 * (`%3D`, `%3A`), hex digits in either case. Rule set 8 took only a literal one, so
 * `password\u003dVALUE` was stored whole.
 */
const SECRET_ASSIGNMENT =
  /\b(?!idempotency_key\s*(?:[:=]|\\{1,16}(?:u003[ad]|x3[ad])|%3[ad]))((?:[A-Za-z0-9]+_){0,8}(?:api[_-]?key|apikey|key|secret|token|password|passwd|passphrase|pwd|credentials?)\s*(?:[:=]|\\{1,16}(?:u003[ad]|x3[ad])|%3[ad]))\s*/gi;

/**
 * An unquoted assignment value, or a quoted one with no matching close within the bound:
 * an optional opening quote, then up to whitespace, a quote, `,` or `;` (rule set 8's
 * value, byte for byte). Sticky, matched in place from the value's start.
 */
const ASSIGNMENT_VALUE = /["'`]?[^"'`\s,;]+/y;
const isQuote = (c: string | undefined) => c === '"' || c === "'" || c === "`";

/**
 * The quoted value of a secret assignment starting at `at`: an opening quote (`"`, `'` or
 * `` ` ``) behind 0 to 16 backslashes, then the value up to its matching closing quote,
 * found within `MAX_QUOTED_VALUE` units across whitespace and line breaks (literal or
 * escaped). The matching close is the test `redactQuotedSecretValues` uses: at escaping
 * depth d the opening quote has 2^d - 1 backslashes, and a quote closes when its backslash
 * run is that many modulo 2^(d+1) (an extra pair is an escaped backslash, an odd excess an
 * inner quote). Returns [value start, value end, end of the closing quote], or undefined
 * when there is no opening quote or no matching close within the bound. One forward walk of
 * at most `MAX_QUOTED_VALUE` + 1 units, so each start stays O(1) in the input's length.
 */
function quotedAssignmentValue(text: string, at: number): [number, number, number] | undefined {
  let bs = 0;
  while (bs < 16 && text[at + bs] === "\\") bs += 1;
  const quote = text[at + bs];
  if (quote !== '"' && quote !== "'" && quote !== "`") return undefined;
  const valueStart = at + bs + 1;
  const period = (bs + 1) & bs ? 0 : 2 * (bs + 1);
  const limit = Math.min(text.length, valueStart + MAX_QUOTED_VALUE + 1);
  for (let i = valueStart; i < limit; i += 1) {
    if (text[i] !== quote) continue;
    const run = backslashesBefore(text, i);
    const own = i - run >= valueStart ? run : i - valueStart;
    if (period ? own % period === bs : own === bs) return [valueStart, i - bs, i + 1];
  }
  return undefined;
}

/**
 * Redact the value of every secret assignment (`SECRET_ASSIGNMENT`), keeping the name, the
 * separator as written (whitespace after it is dropped, as in rule set 8) and, for a quoted
 * value, both quotes: `password="[REDACTED]"`, `\"[REDACTED]\"` one level down.
 *
 * Rule set 9: a quoted value is redacted whole, to its matching closing quote, through
 * whitespace and line breaks (`password="two words"`, `client_secret="a<LF>b"`), when that
 * close is within `MAX_QUOTED_VALUE` units; rule set 8 stopped at the first whitespace or
 * quote and stored the rest. An empty or already redacted quoted value is kept. With no
 * matching close within the bound, and for an unquoted value, the value is rule set 8's
 * (`ASSIGNMENT_VALUE`): an optional quote, then up to whitespace, a quote, `,` or `;`,
 * except that rule set 9 leaves out a backslash run directly before that quote (rule set 8
 * consumed it, breaking the enclosing escape: `\"password=v\"` became `\"password=[REDACTED]"`).
 *
 * Rule set 9 also matches at every position, not only after the previous value, and
 * redacts the union of all the values found. Rule set 8 resumed after each value, so an
 * assignment whose name a value had taken was stored (`password=x{password<TAB>=y` kept
 * `y`), and so would one whose opening quote closed a quoted value before it. Every value
 * rule set 8 redacted is therefore redacted by rule set 9 too: overlapping values merge into
 * one `[REDACTED]`. A match starting inside an unquoted value already found is not measured
 * again (its unquoted value ends where that one does), so each unquoted value is walked
 * once, and each quoted one at most `MAX_QUOTED_VALUE` + 1 units.
 *
 * With `unsplittable`, also report [name start, closing quote end) of every quoted value
 * holding whitespace (a segmenter cut inside one would leave its tail keyless in the next
 * segment) and, with `prefix`, [name start, end of text) of every quoted opener whose
 * close the text ends before the scan bound could decide (fail closed, as
 * `overCapQuotedValueSpans`).
 */
function redactSecretAssignments(text: string, unsplittable?: Array<[number, number]>, prefix = false): string {
  // [start, end, whether the span becomes `[REDACTED]` (else it is dropped: whitespace)].
  const spans: Array<[number, number, boolean]> = [];
  let unquotedEnd = 0;
  SECRET_ASSIGNMENT.lastIndex = 0;
  for (let m = SECRET_ASSIGNMENT.exec(text); m; m = SECRET_ASSIGNMENT.exec(text)) {
    SECRET_ASSIGNMENT.lastIndex = m.index + 1;
    const nameEnd = m.index + m[1].length;
    const at = m.index + m[0].length;
    const quoted = quotedAssignmentValue(text, at);
    if (quoted) {
      const [valueStart, valueEnd, closeEnd] = quoted;
      const value = text.slice(valueStart, valueEnd);
      if (unsplittable && /\s/.test(value)) unsplittable.push([m.index, closeEnd]);
      if (value !== "" && value !== REDACTED) {
        if (at > nameEnd) spans.push([nameEnd, at, false]);
        spans.push([valueStart, valueEnd, true]);
      }
      continue;
    }
    if (unsplittable && prefix) {
      let bs = 0;
      while (bs < 16 && text[at + bs] === "\\") bs += 1;
      if (isQuote(text[at + bs]) && at + bs + 1 + MAX_QUOTED_VALUE + 1 > text.length) unsplittable.push([m.index, text.length]);
    }
    if (at < unquotedEnd) continue;
    ASSIGNMENT_VALUE.lastIndex = at;
    const value = ASSIGNMENT_VALUE.exec(text);
    let end = value ? at + value[0].length : at;
    // Keep a backslash run that escapes the quote ending the value: it belongs to the
    // enclosing escaped string (`\"password=v\"` one level down), not to the value.
    if (isQuote(text[end])) while (end > at && text[end - 1] === "\\") end -= 1;
    const bodyStart = at + (isQuote(text[at]) ? 1 : 0);
    // Nothing to redact, or a value already redacted (kept so a second pass is a no-op).
    if (end <= bodyStart || text.slice(bodyStart, end) === REDACTED) continue;
    spans.push([nameEnd, end, true]);
    unquotedEnd = end;
  }
  if (spans.length === 0) return text;
  spans.sort((a, b) => a[0] - b[0] || b[1] - a[1]);
  let out = "";
  let last = 0;
  for (let i = 0; i < spans.length; ) {
    let [start, end, redact] = spans[i]!;
    for (i += 1; i < spans.length && spans[i]![0] < end; i += 1) {
      end = Math.max(end, spans[i]![1]);
      redact ||= spans[i]![2];
    }
    out += text.slice(last, start) + (redact ? REDACTED : "");
    last = end;
  }
  return out + text.slice(last);
}

const SECRET_PATTERNS: Rule[] = [
  // Rule set 5: over-bound quoted secret values, measured on the text before any rule
  // below can shrink them; rule set 6: and values that hold a private-key boundary
  // paired across their edge. See `redactOverCapQuotedValues`.
  redactOverCapQuotedValues,
  // PEM private keys (PKCS#1/#8, EC, OpenSSH, encrypted) and armored PGP private key
  // blocks (`-----BEGIN PGP PRIVATE KEY BLOCK-----`), each up to its footer.
  [
    // A block with no footer (a truncated paste, a key split across records) is
    // redacted to the end of the string rather than kept.
    /-----BEGIN [A-Z ]*PRIVATE KEY(?: BLOCK)?-----[\s\S]*?(?:-----END [A-Z ]*PRIVATE KEY(?: BLOCK)?-----|$)/g,
    REDACTED,
  ],
  [/(^|[\s"'(=:/])\/Users\/[^/\s]+(?=\/)/g, "$1~"],
  [/(^|[\s"'(=:/])\/home\/[^/\s]+(?=\/)/g, "$1~"],
  [/(^|[\s"'(=])([A-Z]:\\Users\\[^\\\s]+)(?=\\)/gi, "$1~"],
  // The token runs to the end of its characters, so a trailing `-`, `_` or `=` padding is
  // part of it (a `\b` end used to leave it behind); a sentence-ending dot is not.
  [
    /\bBearer\s+[A-Za-z0-9._~+/=-]{15,}[A-Za-z0-9_~+/=-](?![A-Za-z0-9_~+/=-])/gi,
    `Bearer ${REDACTED}`,
  ],
  // SendGrid API keys (`SG.` + 22 + `.` + 43). Ahead of the prefix rules below: either
  // segment can start with or contain `sk-`/`ghp_`-like text, and a prefix rule run first
  // would redact only that tail and leave the start of the key behind.
  [/\bSG\.[A-Za-z0-9_-]{22}\.[A-Za-z0-9_-]{43}(?![A-Za-z0-9_-])/g, REDACTED],
  // Provider key prefixes. The hyphen form covers OpenAI-style `sk-…` (`sk-proj-…`) and
  // Anthropic `sk-ant-…`; the underscore form
  // covers Stripe-style `sk_live_…`/`sk_test_…` (and rk_/pk_) which the hyphen pattern misses.
  // These literal-prefix rules start at `(?<![A-Za-z0-9_-])`, not `\b`: `\b` also fires
  // after a `-` inside a longer token, so `-sk-…`/`-ghp_…` in the body of a later
  // full-shape key (GitLab, Slack, Capy, Google, a JWT) was redacted alone and the key's
  // start leaked. The lookbehind is `\b` plus `-` (`_` and alphanumerics already fail
  // `\b`); a prefix glued to a token by `-` (`word-sk-…`) is taken later instead, with
  // the token it is glued to, by `redactGluedPrefixes` once the full-shape rules have run.
  [/(?<![A-Za-z0-9_-])(?:sk|rk|pk|ak)-[A-Za-z0-9_-]{20,}(?![A-Za-z0-9_-])/g, REDACTED],
  [/(?<![A-Za-z0-9_-])(?:(?:sk|rk|pk)_(?:live|test)_|whsec_)[A-Za-z0-9]{10,}\b/g, REDACTED],
  [/(?<![A-Za-z0-9_-])gh[pousr]_[A-Za-z0-9_]{20,}\b/g, REDACTED],
  // AWS access/secret-access key IDs (AKIA long-term, ASIA temporary).
  [/(?<![A-Za-z0-9_-])(?:AKIA|ASIA)[0-9A-Z]{16}\b/g, REDACTED],
  // GitHub fine-grained personal access tokens (`github_pat_…`); the classic and app
  // tokens (`ghp_`, `gho_`, `ghu_`, `ghs_`, `ghr_`) are the rule above.
  [/(?<![A-Za-z0-9_-])github_pat_[A-Za-z0-9_]{22,}/g, REDACTED],
  // GitLab personal/project/group access tokens.
  [/(?<![A-Za-z0-9_-])glpat-[A-Za-z0-9_-]{20,}/g, REDACTED],
  // npm access tokens (`npm_` + 36).
  [/(?<![A-Za-z0-9_-])npm_[A-Za-z0-9]{36}\b/g, REDACTED],
  // Hugging Face user access tokens (`hf_` + 34 letters).
  [/(?<![A-Za-z0-9_-])hf_[A-Za-z]{34}\b/g, REDACTED],
  // Cloudflare scannable credentials: `cfk_` (Global API Key), `cfut_` (user API token)
  // and `cfat_` (account API token), each a 40-character alphanumeric body and an
  // 8-character hex CRC32 checksum (53 characters for `cfut_`/`cfat_`). The body is taken
  // as 40 or more so a longer variant is still caught; the prefix keeps it tight.
  // Legacy unprefixed Cloudflare credentials (37-hex Global API Keys, 40-character API
  // tokens) have no shape of their own and are caught next to their name instead:
  // `X-Auth-Key: …`, `CLOUDFLARE_API_TOKEN=…`, `"api_token": "…"`, `Bearer …`.
  [/(?<![A-Za-z0-9_-])cf(?:k|ut|at)_[A-Za-z0-9]{40,}/g, REDACTED],
  // Slack tokens (xoxb/xoxp/xoxa/xoxr/xoxs/xoxe-… and app-level xapp-…).
  [/(?<![A-Za-z0-9_-])(?:xox[baprse]|xapp)-[A-Za-z0-9-]{10,}(?![A-Za-z0-9_-])/g, REDACTED],
  // Slack incoming webhook URLs carry their credential in the path; keep the host.
  [
    /(hooks\.slack\.com\/(?:services|workflows|triggers)\/)[A-Za-z0-9_/-]{8,}/g,
    `$1${REDACTED}`,
  ],
  // Capy API keys (`capy_…`), Cursor/Grok Bot routine keys (`crsr_…`) and Agent Relay
  // tokens (`relay_ag_…`, `relay_pa_…`, `relay_ws_…`, `relay_id_…`). Capy and Grok Bot
  // transcripts are uploaded to history, and a turn that set up a routine can echo its key.
  [/(?<![A-Za-z0-9_-])(?:capy|crsr)_[A-Za-z0-9_-]{16,}/g, REDACTED],
  [/(?<![A-Za-z0-9_-])relay_(?:ag|pa|ws|id)_[A-Za-z0-9._-]{16,}/g, REDACTED],
  // Capy automation webhook URLs carry their credential in the path: the URL itself is
  // the secret. Keep the host and route so the reference stays diagnosable. The last
  // character can't be a dot, so a sentence-ending period survives.
  [
    /(\/webhooks\/automations\/)[A-Za-z0-9._~-]{7,}[A-Za-z0-9_~-]/g,
    `$1${REDACTED}`,
  ],
  // Bare JWTs (`header.payload.signature`, header and payload base64url JSON, so `eyJ`),
  // e.g. a Capy session token echoed from /etc/capy/session-token. Matched as maximal
  // token runs (each character is consumed once, so it is linear) and split in code, so
  // there is no length cap a large token could slip past.
  [/[A-Za-z0-9_.-]{35,}/g, redactJwtsInRun],
  // Service-local relayhistory tokens. Distinct from URL userinfo / Bearer.
  [/(?<![A-Za-z0-9_-])rth_(?:at|rt|st)_[A-Za-z0-9_]{8,}\b/g, REDACTED],
  // OAuth authorize URLs: keep host+path so "stuck at auth.openai.com/oauth/authorize"
  // remains diagnosable, redact the challenge/state/secret query values.
  [
    /([?&](?:code_challenge|code_verifier|state|client_secret|access_token|id_token|refresh_token|assertion)=)[^&\s]*/gi,
    `$1${REDACTED}`,
  ],
  // Google API keys (AIza + 35 chars).
  [/(?<![A-Za-z0-9_-])AIza[0-9A-Za-z_-]{30,}(?![A-Za-z0-9_-])/g, REDACTED],
  // The literal-prefix rules again, for a prefix glued to a longer token by `-`. Last of
  // the token rules: see `redactGluedPrefixes`.
  redactGluedPrefixes,
  // Quoted secret keys in every JSON/YAML/Python form, at any escaping depth:
  // `"password": "…"`, `\"api_key\": \"…\"` (a JSON document inside a tool-result string),
  // `'token' => '…'`. The value is redacted, the key and quotes kept. See
  // `redactQuotedSecretValues`.
  redactQuotedSecretValues,
  // `Authorization: Basic …` / `Token …` / `Bot …` and other non-Bearer schemes (Bearer is
  // its own rule above): the credential after the scheme, keeping the scheme.
  [
    /(\bauthorization(?![A-Za-z0-9])[^A-Za-z0-9\r\n]{1,8}(?:Basic|Token|Bot|Digest|Negotiate|NTLM)\s+)[A-Za-z0-9._~+/=-]{8,}/gi,
    `$1${REDACTED}`,
  ],
  // `<name>=<value>` / `<name>: <value>` secret assignments: see `redactSecretAssignments`.
  redactSecretAssignments,
  // Fallback: a generated-looking token (see `looksGenerated`) directly after a secret
  // name with only punctuation or a little whitespace between them, in shapes the
  // assignment rules do not parse: `--token <t>`, `password -> <t>`,
  // `API key <t>`. Only next to a secret name, so hashes and ids elsewhere are kept. The
  // token is bounded (a longer run is left whole) so each start stays O(1).
  // The gap may not start with `/`, `.` or `-`: there the name is a path segment, a
  // member or a word in a branch name (`src/token/TokenRefreshService.java`,
  // `token.getAccessTokenSilently()`, `feature/secret-RotateKeys2026`), not a label.
  [
    /(?<![A-Za-z0-9])((?:password|passwd|passphrase|secret|token|credentials?|api[_ -]?key|access[_ -]?key|private[_ -]?key|secret[_ -]?key|client[_ -]?secret|auth[_ -]?token|session[_ -]?token)(?![A-Za-z0-9_./-])[^A-Za-z0-9\r\n]{1,8})([A-Za-z0-9_+/=.-]{20,512})(?![A-Za-z0-9_+/=.-])/gi,
    (match, lead, token) =>
      looksGenerated(token as string) ? `${lead}${REDACTED}` : match,
  ],
  // A URL scheme is short by definition — RFC 3986 registers nothing close to 32 chars.
  // Leaving the scheme `[a-z0-9+.-]*` unbounded made this pattern QUADRATIC: on any long
  // run of scheme-legal characters the engine consumed to end-of-string hunting for `://`,
  // backtracked the whole way, then repeated from the next offset. That is O(n²), and it is
  // reachable from a single prompt: a 128 KB `AAAA…` prompt cost 21s of CPU here, which took
  // the Worker past its limit (Cloudflare 1102) and 503'd every push from the machine that
  // had it queued — permanently, since a failed batch never advances the cursor.
  // Bounding the scheme makes each start position O(1) and the whole pass linear.
  [/([a-z][a-z0-9+.-]{0,31}:\/\/[^:\s/@]+:)[^@\s/]+(@)/gi, `$1${REDACTED}$2`],
  // Token-only userinfo (`https://<token>@github.com/o/r`) is a credential too. Redact it
  // HERE, keeping the `@host`, rather than leaving it to the email rule below, which would
  // swallow the host along with the token and make the remote undiagnosable. `git@` is
  // the conventional, non-secret SSH user (`ssh://git@github.com/o/r`) and is kept.
  // Userinfo can never contain `?` or `#` (RFC 3986), so an `@` in a query or fragment
  // (`https://host/?to=bob@x.io`) is not mistaken for userinfo and the host survives.
  // Same bounded-scheme shape as the pattern above, so it is linear for the same reason.
  [/([a-z][a-z0-9+.-]{0,31}:\/\/)(?!git@)[^:\s/@?#]+(@)/gi, `$1${REDACTED}$2`],
  // Bounded for the same reason as the URL scheme above, and it is the same failure:
  // `[A-Z0-9._%+-]+` with no upper bound consumed every run of dotted/base64-ish text to
  // end-of-string looking for an `@`, backtracked, then repeated from the next word
  // boundary — quadratic on exactly the kind of blob that shows up in a captured prompt.
  // RFC 5321 caps a local part at 64 characters and a domain at 255, so bounding here
  // costs no real address.
  //
  // The leading lookahead keeps scp-style git remotes (`git@github.com:Org/repo.git`)
  // intact: redacting them turned every SSH remote into `[REDACTED]:Org/repo.git`. Only
  // the conventional, non-secret `git` SSH user followed by `:path` is exempt; any other
  // local part is a real address and stays redacted even when a colon follows it
  // (`bob@example.com:notes`), because a `:path` suffix alone does not make text a remote.
  // The trailing `\.?[A-Z0-9-]` check stops the engine from backtracking to a shorter
  // host so a match always covers the whole address.
  // The leading lookbehind leaves URL userinfo to the userinfo rules above: the only
  // userinfo they keep is `git@` (`ssh://git@github.com/o/r`), which is not an address.
  // Every lookaround is bounded (the remote check by the same RFC limits as the match),
  // so each start position stays O(1) and the pass stays linear.
  [
    /(?<!:\/\/)\b(?!git@[A-Z0-9.-]{1,255}\.[A-Z]{2,24}:[\w~./-])[A-Z0-9._%+-]{1,64}@[A-Z0-9.-]{1,255}\.[A-Z]{2,24}\b(?!\.?[A-Z0-9-])/gi,
    REDACTED,
  ],
  // Phone numbers, only in unambiguous shapes so IDs, dates and counts survive:
  // international with a leading `+` (E.164, 8-15 digits, optional separators), and
  // North American numbers with separators: (415) 555-0123, 415-555-0123, 415.555.0123.
  // The whole run after the `+` is taken: digit groups joined by one separator, with
  // parenthesised groups anywhere (`+44 (20) 7946 0958`, `+44 20 (7946) 0958`). It is
  // redacted if it holds at least 8 digits, so short labels survive; a run holding two
  // adjacent numbers is redacted whole rather than leaving the second one visible.
  // Groups are delimited, so the pattern cannot backtrack exponentially.
  [
    /(^|[^\w+])\+[1-9]\d*(?:[ .-]?\(\d{1,4}\)\d*|[ .-]\d+)*(?!\w|[.-]?\d)/g,
    (match, lead) =>
      (match.match(/\d/g)?.length ?? 0) >= 8 ? `${lead}${REDACTED}` : match,
  ],
  [
    /(^|[^\w.+-])(?:\([2-9]\d{2}\) ?|[2-9]\d{2}[.-])[2-9]\d{2}[.-]\d{4}(?!\w|[.-]?\d)/g,
    `$1${REDACTED}`,
  ],
  // Payment card numbers, after the phone rules: a card rule that ran first could
  // redact the tail of a phone plus the number beside it and leave the rest of the
  // phone unmatched. 13-19 digits, plain or grouped by one consistent space or
  // hyphen, with a known issuer prefix and length and a valid Luhn check digit
  // (`isCardNumber`). The match is a whole run of digit groups (so a card is never taken
  // out of a longer number, a decimal or a word); `redactCardsInRun` then redacts every
  // card in it, so an adjacent expiry date, CVV or second card cannot hide one.
  [
    /(^|[^\w.+-])(\d+(?:[ -]\d+)*)(?!\w|[.-]\d)/g,
    (_match, lead, run) => `${lead}${redactCardsInRun(run as string)}`,
  ],
];

/**
 * Issuer prefixes and lengths for the card rule: Visa, Mastercard (51-55 and 2221-2720),
 * American Express, Diners Club, JCB, Discover and UnionPay. Lengths are per brand so
 * that, for example, an 18-digit ID starting with 2 is never taken for a Mastercard.
 */
function knownIssuer(d: string): boolean {
  const n = d.length;
  if (n < 13 || n > 19) return false;
  const c = (k: number) => d.charCodeAt(k) - 48;
  const prefixes = [0, c(0), c(0) * 10 + c(1), c(0) * 100 + c(1) * 10 + c(2)];
  prefixes.push((prefixes[3] as number) * 10 + c(3));
  const p = (k: number) => prefixes[k] as number;
  const visa = d[0] === "4" && (n === 13 || n === 16 || n === 19);
  const mastercard =
    ((p(2) >= 51 && p(2) <= 55) || (p(4) >= 2221 && p(4) <= 2720)) && n === 16;
  const amex = (p(2) === 34 || p(2) === 37) && n === 15;
  const diners =
    ((p(3) >= 300 && p(3) <= 305) || [36, 38, 39].includes(p(2))) &&
    (n === 14 || n === 16);
  const jcb = p(4) >= 3528 && p(4) <= 3589 && n >= 16 && n <= 19;
  const discoverUnionPay =
    (p(4) === 6011 ||
      (p(3) >= 644 && p(3) <= 649) ||
      p(2) === 65 ||
      p(2) === 62) &&
    n >= 16 &&
    n <= 19;
  return visa || mastercard || amex || diners || jcb || discoverUnionPay;
}

/** A known issuer prefix and length (`knownIssuer`) and a valid Luhn check digit. */
function isCardDigits(digits: string): boolean {
  if (!knownIssuer(digits)) return false;
  let sum = 0;
  for (let i = 0; i < digits.length; i++) {
    let d = digits.charCodeAt(digits.length - 1 - i) - 48;
    if (i % 2 === 1) {
      d *= 2;
      if (d > 9) d -= 9;
    }
    sum += d;
  }
  return sum % 10 === 0;
}

/**
 * Whether `text` (digits, optionally grouped by one consistent space or hyphen) is a
 * plausible payment card number: a known issuer prefix and length and a valid Luhn
 * check digit.
 */
export function isCardNumber(text: string): boolean {
  let digits = "";
  let sep = "";
  for (const c of text) {
    if (c >= "0" && c <= "9") digits += c;
    else if ((c !== " " && c !== "-") || (sep && c !== sep)) return false;
    else sep = c;
  }
  return isCardDigits(digits);
}

/**
 * Redact every card number in `run` (digit groups separated by single spaces or
 * hyphens). A card starts at a group and spans whole groups joined by one consistent
 * separator; at each group the longest valid card wins, and groups that start none are
 * kept as they are. Each start looks at most 19 digits ahead, so this is linear.
 */
function redactCardsInRun(run: string): string {
  const groups = run.split(/[ -]/);
  const seps = run.replace(/\d+/g, "");
  let out = "";
  let i = 0;
  while (i < groups.length) {
    let end = -1;
    let digits = "";
    // Every issuer prefix starts with 2-6.
    const first = (groups[i] as string)[0] as string;
    for (
      let j = first >= "2" && first <= "6" ? i : groups.length;
      j < groups.length;
      j++
    ) {
      if (j > i + 1 && seps[j - 1] !== seps[i]) break;
      digits += groups[j];
      if (digits.length > 19) break;
      if (digits.length >= 13 && isCardDigits(digits)) end = j;
    }
    out += end === -1 ? groups[i] : REDACTED;
    const next = end === -1 ? i + 1 : end + 1;
    if (next < groups.length) out += seps[next - 1];
    i = next;
  }
  return out;
}

/**
 * A JSON number that is a card number becomes `[REDACTED]`. Only safe integers are
 * checked: a larger number has already lost digits in `JSON.parse`, so the exact card
 * number never reaches this point.
 */
export function scrubNumber(value: number): number | string {
  return Number.isSafeInteger(value) && value > 0 && isCardNumber(String(value))
    ? REDACTED
    : value;
}

/**
 * Hard bound on a single string handed to the scrubber. Every path into the store —
 * `content`, the task fields, and each string inside `record` via `scrubJson` — funnels
 * through `scrubText`, so bounding here bounds the whole ingest.
 *
 * 256 KiB is ~10x the p99.9 of real captured prompts (23 KB; nothing in a recent 6k-row
 * sample exceeded 33 KB), so this truncates no genuine history. It exists so that a single
 * pathological record cannot again wedge a machine's push forever the way the quadratic
 * scheme pattern did: even at Cloudflare's 100 MB request-body ceiling the scrub pass now
 * stays well inside the Worker CPU limit.
 */
export const MAX_SCRUB_CHARS = 262_144;

const TRUNCATION_NOTE = "[relayhistory: truncated";

const PEM_BEGIN = /-----BEGIN [A-Z ]*PRIVATE KEY(?: BLOCK)?-----/g;
const PEM_END = /-----END [A-Z ]*PRIVATE KEY(?: BLOCK)?-----/g;
/** `PEM_BEGIN`, matched in place. */
const PEM_BEGIN_AT = /-----BEGIN [A-Z ]*PRIVATE KEY(?: BLOCK)?-----/y;

/**
 * Start of the first private-key block in `text` that the PEM rule cannot
 * close (its `-----END ...-----` footer is missing), or -1. Mirrors how the
 * lazy PEM pattern pairs each header with the next footer, left to right.
 */
function unclosedPrivateKeyStart(text: string): number {
  let pos = 0;
  for (;;) {
    PEM_BEGIN.lastIndex = pos;
    const begin = PEM_BEGIN.exec(text);
    if (!begin) return -1;
    PEM_END.lastIndex = begin.index + begin[0].length;
    const end = PEM_END.exec(text);
    if (!end) return begin.index;
    pos = end.index + end[0].length;
  }
}

/**
 * Truncate over-long input BEFORE the patterns run, cutting only after whitespace.
 *
 * Cutting mid-token would leave the leading fragment of a credential in the retained
 * prefix — short enough that no `{20,}`-style pattern matches it, so the scrubber would
 * silently stop protecting the very thing it exists to protect. Apart from the PEM rule,
 * no rule's match contains whitespace except as a separator before the secret (Bearer,
 * `key = value`), as a delimiter before it (home paths), or between the digit groups of
 * a card or phone number. So the cut falls only right after whitespace and never
 * between two digit groups (a digit or `)` before the whitespace, a digit or `(` after
 * it), which leaves every secret either wholly retained (and therefore scrubbed) or
 * wholly dropped.
 *
 * The cut therefore rewinds to the last whitespace before the limit however far back
 * that is. An input with no whitespace at all is one unbroken run (minified JSON, a
 * base64 dump) and is dropped whole rather than cut inside, because a credential can
 * sit anywhere in such a run. Cuts after whitespace never split a surrogate pair, since
 * every JavaScript whitespace character is in the BMP.
 *
 * A private key is the one multi-line secret: the PEM rule needs both delimiters, so a
 * block whose footer falls past the cut is withheld from its header on.
 *
 * The truncation is always announced in band so a reader can tell a truncated record
 * from a short one.
 */
/** Whether `value` may be cut before index `at` (see `boundForScrub`). */
function safeCut(value: string, at: number): boolean {
  return (
    /\s/.test(value[at - 1] as string) &&
    !(/[\d)]/.test(value[at - 2] ?? "") && /[\d(]/.test(value[at] ?? ""))
  );
}

function boundForScrub(value: string): string {
  if (value.length <= MAX_SCRUB_CHARS) {
    return value;
  }
  let cut = MAX_SCRUB_CHARS;
  while (cut > 0 && !safeCut(value, cut)) {
    cut -= 1;
  }
  const unclosed = unclosedPrivateKeyStart(value.slice(0, cut));
  if (unclosed !== -1) {
    cut = unclosed;
  }
  const dropped = value.length - cut;
  return `${value.slice(0, cut)}\n${TRUNCATION_NOTE} ${dropped} characters over the ${MAX_SCRUB_CHARS}-character scrub limit]`;
}

/** Whether `scrubText` will truncate this input (and announce it in band). */
export function exceedsScrubBound(value: string): boolean {
  return value.length > MAX_SCRUB_CHARS;
}

/**
 * Scrub one string. Each string is scrubbed on its own: a secret split across two
 * strings, JSON values or records (a token streamed as separate deltas, a value
 * concatenated only at display time) is not recognized, because neither part matches a
 * rule. Callers that assemble text from parts should scrub the assembled text.
 */
export function scrubText(value: string): string {
  return SECRET_PATTERNS.reduce(
    (text, rule) =>
      typeof rule === "function"
        ? rule(text)
        : text.replace(rule[0], rule[1] as any),
    boundForScrub(value),
  );
}

/**
 * Whether a string value under a key that names a secret (`isSecretKeyName`) is redacted
 * whole, whatever it holds (rule set 5): every non-empty value but one already redacted or
 * a complete one-line 1Password reference (`OP_REFERENCE`, the whole value), the values
 * the quoted key/value rule keeps. The record paths
 * (`scrubJson`, and `scrubRecords` in `lib/pipeline/raw-chunks.ts`) scrub each value on
 * its own, apart from its key, so they decide on the key first, as the quoted rule does
 * in text; rule set 4 kept such a value unless it matched a rule by itself.
 */
export function redactsSecretKeyedValue(value: string): boolean {
  return value !== "" && !keepQuotedValue(value);
}

/** Whether a record key names a secret, for `redactsSecretKeyedValue` (the key as written, trimmed). */
export const isSecretRecordKey = (key: string) => isSecretKeyName(key.trim());

/**
 * Scrub every string value **and key** of a JSON value, and card numbers among its
 * numbers. Two keys that become identical after scrubbing are both kept: later ones get
 * a ` (2)`, ` (3)`… suffix, so no value is silently overwritten and no input is
 * rejected (a rejected record would wedge the push that carries it).
 *
 * Key context first (rule set 5): a string under a key that names a secret
 * (`isSecretRecordKey`, the source key) is redacted whole (`redactsSecretKeyedValue`)
 * before it is scrubbed on its own, and so is every string anywhere below such a key, in
 * arrays and objects alike, at any depth, whatever the keys in between are named: a
 * composite value does not reset the secret context. The structure is kept (keys are
 * still scrubbed and suffixed as everywhere), as are numbers, booleans and null.
 */
export function scrubJson<T>(value: T): T {
  return scrubJsonValue(value, false);
}

function scrubJsonValue<T>(value: T, secretKeyed: boolean): T {
  if (typeof value === "string") {
    return (secretKeyed && redactsSecretKeyedValue(value) ? REDACTED : scrubText(value)) as T;
  }
  if (typeof value === "number") {
    return scrubNumber(value) as T;
  }
  if (Array.isArray(value)) {
    return value.map((item) => scrubJsonValue(item, secretKeyed)) as T;
  }
  if (value && typeof value === "object") {
    const output: Record<string, unknown> = {};
    // Next suffix to try per scrubbed key, so many collisions stay linear.
    const nextSuffix = new Map<string, number>();
    for (const [key, item] of Object.entries(value)) {
      const base = scrubText(key);
      let name = base;
      if (Object.hasOwn(output, name)) {
        let n = nextSuffix.get(base) ?? 2;
        while (Object.hasOwn(output, `${base} (${n})`)) n++;
        name = `${base} (${n})`;
        nextSuffix.set(base, n + 1);
      }
      output[name] = scrubJsonValue(item, secretKeyed || isSecretRecordKey(key));
    }
    return output as T;
  }
  return value;
}

export function scrubRecord<T extends Record<string, unknown>>(
  record: T,
): Record<string, unknown> {
  const scrubbed = scrubJson(record);
  const output: Record<string, unknown> = {};

  for (const [key, value] of Object.entries(scrubbed)) {
    if (key === "confidence") {
      continue;
    }
    if (DEFAULT_RECORD_ALLOWLIST.has(key)) {
      output[key] = stripPromotedConfidence(value);
    }
  }

  return output;
}

function stripPromotedConfidence(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((item) => stripPromotedConfidence(item));
  }
  if (value && typeof value === "object") {
    const output: Record<string, unknown> = {};
    for (const [key, item] of Object.entries(value)) {
      if (key !== "confidence") {
        output[key] = stripPromotedConfidence(item);
      }
    }
    return output;
  }
  return value;
}

const DEFAULT_RECORD_ALLOWLIST = new Set([
  "id",
  "eventId",
  "messageId",
  "requestId",
  "fingerprint",
  "idFingerprint",
  "source",
  "lens",
  "sessionId",
  "kind",
  "type",
  "ts",
  "startTs",
  "endTs",
  "writtenAt",
  "significance",
  "confidence",
  "tags",
  "model",
  "provider",
  "usage",
  "costUsdMicros",
  "toolName",
  "toolTarget",
  "toolStatus",
  "toolCalls",
  "toolError",
  "activity",
  "stopReason",
  "fidelity",
  "coverage",
  "relationshipType",
  "parentSessionId",
  "rootSessionId",
  "subagentId",
  "actorName",
  "actorRole",
  "filesTouched",
  "filesChanged",
  "codeChurn",
  "trace",
  "decision",
  "task",
  "taskRef",
  "taskTitle",
  "taskDescription",
  "taskStatus",
  "chapterId",
  "agent",
  "status",
  "commits",
  "sizes",
  "truncated",
]);
