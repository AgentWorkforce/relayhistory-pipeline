import { describe, expect, test } from "bun:test";
import { dateBounds, parseSessionQuery, queryFingerprint, queryWords, type SessionQueryTerms } from "../list-query.ts";

const DAY = 86_400_000;
const at = (d: string) => Date.parse(`${d}T00:00:00Z`);

function terms(q: string): SessionQueryTerms {
  const p = parseSessionQuery(q);
  if (p.kind !== "terms") throw new Error(`expected terms for ${q}: ${JSON.stringify(p)}`);
  return p.terms;
}
const reason = (q: string) => {
  const p = parseSessionQuery(q);
  return p.kind === "terms" ? "terms" : `${p.kind}:${p.reason}`;
};

describe("queryWords (tokenizer)", () => {
  test("splits on whitespace outside quotes; quotes stay in the word", () => {
    expect(queryWords("  alpha\tbeta\n gamma ")).toEqual(["alpha", "beta", "gamma"]);
    expect(queryWords('title:"two words" next')).toEqual(['title:"two words"', "next"]);
  });
  test("an escaped quote does not toggle quoting; an escaped backslash does not escape", () => {
    expect(queryWords('"a \\" b" c')).toEqual(['"a \\" b"', "c"]);
    expect(queryWords('"a\\\\" b')).toEqual(['"a\\\\"', "b"]);
  });
  test("an unterminated quote runs to the end", () => {
    expect(queryWords('"open quote here')).toEqual(['"open quote here']);
  });
  test("Unicode whitespace splits (U+3000, U+0085) but U+FEFF does not", () => {
    expect(queryWords("alpha　beta\u0085gamma")).toEqual(["alpha", "beta", "gamma"]);
    expect(queryWords("alpha﻿beta")).toEqual(["alpha﻿beta"]);
  });
});

describe("parseSessionQuery", () => {
  test("free words are lowercased, normalized and AND (deduplicated)", () => {
    const t = terms("Deploy  ROLLBACK deploy");
    expect(t.free).toEqual(["deploy", "rollback"]);
    expect(t.notFree).toEqual([]);
  });
  test("a quoted phrase is one free term with its quotes removed and \\\" unescaped", () => {
    expect(terms('"fix the bug"').free).toEqual(["fix the bug"]);
    expect(terms('"say \\"hi\\" now"').free).toEqual(['say "hi" now']);
  });
  test("-word excludes; a lone - is a (too short) free word", () => {
    const t = terms("-flaky alpha");
    expect(t.notFree).toEqual(["flaky"]);
    expect(t.free).toEqual(["alpha"]);
    expect(reason("-")).toBe("bad:a search word needs at least 3 characters");
  });
  test("fields: title, project, harness/source, agent, id (case-insensitive field names)", () => {
    const t = terms("TITLE:Login project:Web harness:codex Source:claude agent:Reviewer id:ABC-1 -title:wip -id:x");
    expect(t.title).toEqual(["login"]);
    expect(t.notTitle).toEqual(["wip"]);
    expect(t.project).toEqual(["web"]);
    expect(t.harness).toEqual(["codex", "claude"]);
    expect(t.agent).toEqual(["reviewer"]);
    expect(t.id).toEqual(["abc-1"]);
    expect(t.notId).toEqual(["x"]);
  });
  test("short field values are allowed (only free words need 3 characters)", () => {
    expect(terms("id:x title:a").title).toEqual(["a"]);
  });
  test("a word ending in : takes the next word (no colon, or quoted)", () => {
    expect(terms("title: login").title).toEqual(["login"]);
    expect(terms('title: "two words"').title).toEqual(["two words"]);
    // The next word has a colon and is not quoted: no join; `title:` alone has no value.
    expect(reason("title: project:web")).toBe("bad:add a value after title:");
  });
  test("an unknown field is one free word, colon included", () => {
    expect(terms("foo:bar").free).toEqual(["foo:bar"]);
    expect(terms("http://example.test/x").free).toEqual(["http://example.test/x"]);
  });
  test("@name is an agent term, as free word or agent: value; -@name excludes", () => {
    const t = terms("@Reviewer agent:@@lead -@bot");
    expect(t.agent).toEqual(["reviewer", "lead"]);
    expect(t.notAgent).toEqual(["bot"]);
    expect(t.free).toEqual([]);
    expect(reason("@")).toBe("bad:add an agent name after @");
    // A quoted @ value is an agent term too (the quotes go first).
    expect(terms('"@two words"').agent).toEqual(["two words"]);
  });
  test("is:active / is:inactive and their exclusions", () => {
    expect(terms("is:active").active).toBe(true);
    expect(terms("is:Inactive").active).toBe(false);
    expect(terms("-is:inactive").active).toBe(true);
    expect(terms("-is:active").active).toBe(false);
    expect(terms("is:active -is:inactive").active).toBe(true);
    expect(reason("is:active is:inactive")).toBe("bad:choose either is:active or is:inactive");
    expect(reason("is:active -is:active")).toBe("bad:choose either is:active or is:inactive");
    expect(reason("is:busy")).toBe("bad:use is:active, is:inactive, is:on-relay or is:off-relay");
  });
  test("unsupported terms are explicit 501 reasons, excluded or not", () => {
    expect(reason("path:/tmp")).toBe("unsupported:q_filter_path");
    expect(reason("-path:/tmp")).toBe("unsupported:q_filter_path");
    expect(reason("upload:uploaded")).toBe("unsupported:q_filter_upload");
    expect(reason("-upload:local")).toBe("unsupported:q_filter_upload");
    expect(reason("is:on-relay")).toBe("unsupported:q_filter_is_on-relay");
    expect(reason("-is:OFF-relay")).toBe("unsupported:q_filter_is_off-relay");
    expect(reason("-updated:2026-10-01")).toBe("unsupported:q_filter_date_exclusion");
  });
  test("opposite selections and malformed values are 400 before any 501", () => {
    expect(reason("is:on-relay is:off-relay")).toBe("bad:choose either is:on-relay or is:off-relay");
    expect(reason("upload:none upload:uploaded")).toBe("bad:choose compatible upload filters; opposite states cannot be combined");
    expect(reason("upload:local upload:in-progress")).toBe("bad:choose compatible upload filters; opposite states cannot be combined");
    expect(reason("upload:sideways")).toBe("bad:use upload:uploaded, upload:local, upload:in-progress or upload:discovering");
    expect(reason("path:")).toBe("bad:add a value after path:");
    expect(reason('title:""')).toBe("bad:add a value after title:");
  });
  test("harness values must be harness names", () => {
    expect(reason("harness:Not/A/Harness")).toBe("bad:harness: must name a harness");
    expect(reason("source:-x")).toBe("bad:source: must name a harness");
  });
  test("bounds: zero terms, too many terms, too short / too long values", () => {
    expect(reason("")).toBe("bad:q has no search terms");
    expect(reason("   \t ")).toBe("bad:q has no search terms");
    expect(reason(Array.from({ length: 16 }, (_, i) => `word${i}`).join(" "))).toBe("terms");
    expect(reason(Array.from({ length: 17 }, (_, i) => `word${i}`).join(" "))).toBe("bad:q accepts at most 16 terms");
    expect(reason("ab")).toBe("bad:a search word needs at least 3 characters");
    expect(reason('"a b"')).toBe("bad:a search word needs at least 3 characters");
    expect(reason("x".repeat(201))).toBe("bad:a q term must be 200 characters or fewer");
    expect(reason(`title:${"x".repeat(201)}`)).toBe("bad:a q term must be 200 characters or fewer");
    expect(reason(`title:${"x".repeat(200)}`)).toBe("terms");
  });
  test("LIKE metacharacters are passed through as plain values", () => {
    expect(terms("100% under_score back\\slash").free).toEqual(["100%", "under_score", "back\\slash"]);
  });
});

describe("dates", () => {
  test("every form", () => {
    expect(dateBounds("2026-10-01", "after")).toEqual([at("2026-10-02"), Infinity]);
    expect(dateBounds("2026-10-01", "before")).toEqual([-Infinity, at("2026-10-01")]);
    expect(dateBounds("2026-10-01..2026-10-11", "updated")).toEqual([at("2026-10-01"), at("2026-10-12")]);
    expect(dateBounds("*..2026-10-11", "updated")).toEqual([-Infinity, at("2026-10-12")]);
    expect(dateBounds("2026-10-01..", "updated")).toEqual([at("2026-10-01"), Infinity]);
    expect(dateBounds(">=2026-10-01", "updated")).toEqual([at("2026-10-01"), Infinity]);
    expect(dateBounds(">2026-10-01", "updated")).toEqual([at("2026-10-01") + DAY, Infinity]);
    expect(dateBounds("<=2026-10-01", "updated")).toEqual([-Infinity, at("2026-10-01") + DAY]);
    expect(dateBounds("<2026-10-01", "updated")).toEqual([-Infinity, at("2026-10-01")]);
    expect(dateBounds("2026-10-01", "updated")).toEqual([at("2026-10-01"), at("2026-10-02")]);
  });
  test("malformed dates and empty ranges", () => {
    for (const v of ["2026-02-30", "2026-1-01", "26-10-01", "2026-10-01T00:00", "0000-01-01", "2026-10-11..2026-10-01", "2026-10-01..2026-09-30", "today", "2026-10-01..x"])
      expect(dateBounds(v, "updated")).toBeUndefined();
    // Both sides open is a valid, unbounded range (it filters nothing).
    expect(dateBounds("..", "updated")).toEqual([-Infinity, Infinity]);
    expect(dateBounds("*..*", "updated")).toEqual([-Infinity, Infinity]);
    expect(dateBounds("2026-10-01..2026-10-01", "updated")).toEqual([at("2026-10-01"), at("2026-10-02")]);
    expect(reason("updated:2026-02-30")).toStartWith("bad:use a date like");
    expect(reason("after:>=2026-10-01")).toStartWith("bad:use a date like");
  });
  test("date terms intersect into last-activity bounds; open sides stay unset", () => {
    const t = terms("updated:>=2026-10-01 before:2026-10-20 updated:<=2026-10-25");
    expect(t.lastTsAfter?.toISOString()).toBe("2026-10-01T00:00:00.000Z");
    expect(t.lastTsBefore?.toISOString()).toBe("2026-10-20T00:00:00.000Z");
    expect(terms("after:2026-10-01").lastTsBefore).toBeUndefined();
    expect(reason("after:2026-10-05 before:2026-10-05")).toBe("bad:the q date terms do not overlap");
  });
  test("a bound past 9999-12-31 is open", () => {
    const t = terms("updated:9999-12-31");
    expect(t.lastTsAfter?.toISOString()).toBe("9999-12-31T00:00:00.000Z");
    expect(t.lastTsBefore).toBeUndefined();
  });
  test("date-only q is valid (no free words)", () => {
    expect(terms("updated:2026-10-01").free).toEqual([]);
  });
});

describe("queryFingerprint", () => {
  test("32 lowercase hex characters, stable for the same query and filters", () => {
    const a = queryFingerprint(terms("alpha title:x"), { sources: ["codex", "claude"] });
    expect(a).toMatch(/^[0-9a-f]{32}$/u);
    expect(queryFingerprint(terms("alpha title:x"), { sources: ["claude", "codex"] })).toBe(a);
    expect(queryFingerprint(terms("title:x  ALPHA"), { sources: ["codex", "claude"] })).toBe(a);
  });
  test("a different query or a different filter changes it", () => {
    const base = queryFingerprint(terms("alpha"), {});
    expect(queryFingerprint(terms("beta"), {})).not.toBe(base);
    expect(queryFingerprint(terms("-alpha"), {})).not.toBe(base);
    expect(queryFingerprint(terms("title:alpha"), {})).not.toBe(base);
    expect(queryFingerprint(terms("alpha is:active"), {})).not.toBe(base);
    expect(queryFingerprint(terms("alpha updated:2026-10-01"), {})).not.toBe(base);
    expect(queryFingerprint(terms("alpha"), { sources: ["codex"] })).not.toBe(base);
    expect(queryFingerprint(terms("alpha"), { userIds: ["user-1"] })).not.toBe(base);
    expect(queryFingerprint(terms("alpha"), { agent: "lead" })).not.toBe(base);
    expect(queryFingerprint(terms("alpha"), { firstTsAfter: new Date(at("2026-10-01")) })).not.toBe(base);
  });
});
