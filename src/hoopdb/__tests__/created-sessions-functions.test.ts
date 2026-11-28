/**
 * createdSessionsFunctions: the manifest key of every sessions function a held step creates. A parameter DEFAULT is not
 * part of the signature, so `fn(a text, b jsonb DEFAULT NULL)` is `sessions.fn(text,jsonb)`. Defaults can hold commas,
 * parentheses and casts; arguments are split at top-level commas first. OUT parameters, and a default with a quoted
 * comma (the splitter does not track quotes), still under-credit: the safe direction.
 */
import { describe, expect, test } from "bun:test";
import { createdSessionsFunctions } from "../migrate-guards.ts";

describe("createdSessionsFunctions", () => {
  test("a parameter DEFAULT (keyword or =) is dropped from the key; the types are kept", () => {
    const sql = `CREATE OR REPLACE FUNCTION sessions.reader(org text,workspace text,after_cursor jsonb DEFAULT NULL,page_size integer DEFAULT 512) RETURNS jsonb AS $x$ SELECT 1 $x$;
      CREATE FUNCTION sessions.eq(p_a text = 'a', p_b integer DEFAULT (1+2), p_c timestamptz DEFAULT now()) RETURNS void AS $y$ $y$;
      CREATE FUNCTION sessions.plain(p text) RETURNS void AS $z$ $z$;`;
    expect([...createdSessionsFunctions([sql])].sort()).toEqual([
      "sessions.eq(text,integer,timestamp with time zone)",
      "sessions.plain(text)",
      "sessions.reader(text,text,jsonb,integer)",
    ]);
  });

  test("a default containing a quoted comma still under-credits (no manifest key matches): the safe direction", () => {
    const keys = [...createdSessionsFunctions(["CREATE FUNCTION sessions.q(p text DEFAULT 'a,b') RETURNS void AS $q$ $q$;"])];
    expect(keys).not.toContain("sessions.q(text)");
  });

  test("the 062360 reader is credited under its catalogue key", () => {
    const sql = "CREATE OR REPLACE FUNCTION sessions.session_erasure_replay_read(org text,workspace text,source text,fence_session text,\n  after_cursor jsonb DEFAULT NULL,page_size integer DEFAULT 512) RETURNS jsonb";
    expect(createdSessionsFunctions([sql]).has("sessions.session_erasure_replay_read(text,text,text,text,jsonb,integer)")).toBe(true);
  });
});
