import { describe, expect, test } from "bun:test";
import { logLine } from "../log.ts";
import { PG_MESSAGE_CLASSES, PG_MESSAGE_CLASS_TOKENS, pgErrorFields, pgMessageClass, pgWhereFrame } from "../pg-diagnostics.ts";

/** A pg DatabaseError's shape (node-postgres sets these from the server's ErrorResponse). */
const pgError = (fields: Record<string, unknown>) => Object.assign(new Error(String(fields.message ?? "")), fields);
const line = (fields: Record<string, unknown>) => JSON.parse(logLine("error", "pointer index failed", fields)) as Record<string, unknown>;

describe("pg_message_class", () => {
  // Each known message as PostgreSQL reports it, and its token.
  const KNOWN: [string, string][] = [
    ["publication: user fence missing", "publication_user_fence_missing"],
    ["publication: pointer mutation busy", "publication_pointer_mutation_busy"],
    ["publication: pointer mutation requires fresh snapshots", "publication_pointer_fresh_snapshots"],
    ["publication: pointer CAS did not apply", "publication_pointer_cas_not_applied"],
    ["publication: pointer scope busy", "publication_pointer_scope_busy"],
    ["history: carrier hash collision", "history_carrier_hash_collision"],
    ["history: arm pointer tracking before the backfill", "history_arm_pointer_tracking"],
    ["history: pointer references not tracked and backfilled", "history_pointer_refs_untracked"],
    ["history: dangling pointer references; rerun the pointer backfill", "history_dangling_pointer_refs"],
    ["history: 620 is switched off", "history_switched_off"],
    ["history: session_index has an in-flight writer; nothing changed, retry ingest_history_set_live(true)", "history_index_writer_in_flight"],
    ["operator erase: writer requires a fresh read-committed snapshot", "erase_writer_fresh_snapshot"],
    ["operator erase: read-committed transaction required", "erase_read_committed_required"],
    ["acquisition: session_busy", "acquisition_session_busy"],
    ['currval of sequence "session_seq" is not yet defined in this session', "pg_currval_undefined"],
    ["lastval is not yet defined in this session", "pg_lastval_undefined"],
    ['could not obtain lock on row in relation "ingest_fence_owners"', "pg_lock_not_obtained"],
    ['could not obtain lock on relation "session_index"', "pg_lock_not_obtained"],
    ["canceling statement due to lock timeout", "pg_lock_timeout"],
    ["could not serialize access due to concurrent update", "pg_serialization_failure"],
    ["deadlock detected", "pg_deadlock_detected"],
  ];

  test("each known message maps to its token; the table and the test agree", () => {
    for (const [message, token] of KNOWN) expect(pgMessageClass(message)).toBe(token);
    expect(new Set(KNOWN.map(([, t]) => t))).toEqual(new Set(PG_MESSAGE_CLASSES.map((c) => c.token)));
    for (const c of PG_MESSAGE_CLASSES) expect(c.token).toMatch(/^[a-z][a-z0-9_]{0,47}$/);
    expect(PG_MESSAGE_CLASS_TOKENS.has("other")).toBe(true);
  });

  test("unknown, near-miss and non-string messages map to other", () => {
    for (const m of [
      "",
      "something else",
      "publication: user fence missing!", // exact entries need the exact text
      " publication: user fence missing",
      "Publication: user fence missing",
      "buffer gc: user fence missing",
      "role x exists elevated",
      undefined,
      null,
      42,
      {},
    ])
      expect(pgMessageClass(m)).toBe("other");
  });
});

describe("pg_where_frame", () => {
  test("only the first frame, normalised; sessions functions keep their name", () => {
    const where = [
      "PL/pgSQL function sessions.ingest_publication_guard_pointer() line 12 at RAISE",
      'SQL statement "UPDATE sessions.session_index SET manifest_key = \'96bulls/org_secret/v1/ws=ws_secret/...\' WHERE session_id = \'sess-secret\'"',
      "PL/pgSQL function sessions.upsert_session_index(text,text,text,text,text,bigint) line 40 at SQL statement",
    ].join("\n");
    expect(pgWhereFrame(where)).toBe("PL/pgSQL function sessions.ingest_publication_guard_pointer(...) line 12 at RAISE");
    expect(pgWhereFrame("PL/pgSQL function sessions.upsert_session_index(text,text,bigint) line 7 at SQL statement")).toBe(
      "PL/pgSQL function sessions.upsert_session_index(...) line 7 at SQL STATEMENT",
    );
    expect(pgWhereFrame("PL/pgSQL function sessions.ingest_history_sync_pointer() line 3 at FOR over SELECT rows")).toBe(
      "PL/pgSQL function sessions.ingest_history_sync_pointer(...) line 3 at FOR OVER SELECT ROWS",
    );
  });

  test("a non-sessions or quoted function name is other; an unknown statement kind is OTHER", () => {
    expect(pgWhereFrame("PL/pgSQL function public.leak_org_secret() line 2 at RAISE")).toBe("PL/pgSQL function other(...) line 2 at RAISE");
    expect(pgWhereFrame("PL/pgSQL function ingest_guard() line 2 at RAISE")).toBe("PL/pgSQL function other(...) line 2 at RAISE");
    expect(pgWhereFrame('PL/pgSQL function "sessions"."Weird"() line 2 at RAISE')).toBe("PL/pgSQL function other(...) line 2 at RAISE");
    expect(pgWhereFrame("PL/pgSQL function sessions.Upper_Case() line 2 at RAISE")).toBe("PL/pgSQL function other(...) line 2 at RAISE");
    expect(pgWhereFrame("PL/pgSQL function sessions.f() line 2 at frobnicate")).toBe("PL/pgSQL function sessions.f(...) line 2 at OTHER");
  });

  test("a first frame that is not a PL/pgSQL line frame is other; absent where is omitted", () => {
    expect(pgWhereFrame('SQL statement "SELECT \'org_secret\'"\nPL/pgSQL function sessions.f() line 1 at PERFORM')).toBe("other");
    expect(pgWhereFrame('SQL function "f" statement 1')).toBe("other");
    expect(pgWhereFrame("PL/pgSQL function sessions.f() during function entry")).toBe("other");
    expect(pgWhereFrame("PL/pgSQL function sessions.f('org_secret') line 1 at RAISE")).toBe("PL/pgSQL function sessions.f(...) line 1 at RAISE");
    expect(pgWhereFrame(undefined)).toBeUndefined();
    expect(pgWhereFrame("")).toBeUndefined();
  });

  test("a long where-stack is truncated to its first frame", () => {
    const frames = Array.from({ length: 500 }, (_, i) => `PL/pgSQL function sessions.f${i}(text) line ${i + 1} at PERFORM`);
    const out = pgWhereFrame(frames.join("\n"))!;
    expect(out).toBe("PL/pgSQL function sessions.f0(...) line 1 at PERFORM");
    expect(out).not.toContain("\n");
    expect(out).not.toContain("f1(");
  });
});

describe("pgErrorFields and the log allow-list", () => {
  const SECRET = ["org_secret_123", "ws_secret_456", "sess-secret-789", "'quoted value'", '"double quoted"', "hello customer"];
  const loud = pgError({
    code: "55000",
    severity: "ERROR",
    message: "publication: user fence missing for org_secret_123 'quoted value'",
    detail: "Key (org_id)=(org_secret_123) ws_secret_456",
    hint: "sess-secret-789",
    where: 'PL/pgSQL function sessions.ingest_publication_guard_pointer() line 9 at RAISE\nSQL statement "SELECT \'hello customer\' \\"double quoted\\""',
    table: "session_index",
    constraint: "session_index_pkey",
    routine: "exec_stmt_raise",
    schema: "sessions",
    column: "org_id",
  });

  test("the allow-listed fields of a pg error; raw text with ids or quotes never appears", () => {
    const fields = pgErrorFields(loud);
    expect(fields).toEqual({
      pg_table: "session_index",
      pg_constraint: "session_index_pkey",
      pg_routine: "exec_stmt_raise",
      pg_where_frame: "PL/pgSQL function sessions.ingest_publication_guard_pointer(...) line 9 at RAISE",
      pg_message_class: "other", // the text differs from the known message: never logged, never guessed
    });
    const out = logLine("error", "pointer index failed", { stage: "session-indexed", error_code: "55000", ...fields });
    for (const s of SECRET) expect(out).not.toContain(s);
    expect(out).not.toContain("detail");
    expect(out).not.toContain("hint");
  });

  test("the exact known message classes; values that fail a shape are dropped", () => {
    expect(pgErrorFields(pgError({ code: "55000", message: "history: carrier hash collision" }))).toEqual({ pg_message_class: "history_carrier_hash_collision" });
    const odd = pgErrorFields(pgError({ code: "40001", message: "x", table: "Session Index; DROP", constraint: "org_secret'1", routine: "exec stmt", where: 42 }));
    expect(odd).toEqual({ pg_message_class: "other" });
  });

  test("a non-database error adds nothing", () => {
    expect(pgErrorFields(new Error("publication: user fence missing"))).toEqual({});
    expect(pgErrorFields(Object.assign(new Error("x"), { code: "transform_disabled" }))).toEqual({});
    expect(pgErrorFields(Object.assign(new Error("x"), { code: "ECONNRESET" }))).toEqual({});
    expect(pgErrorFields(undefined)).toEqual({});
    expect(pgErrorFields("55000")).toEqual({});
  });

  test("the log allow-list re-checks every value: off-shape or off-vocabulary values are dropped", () => {
    const out = line({
      pg_table: "org_secret 'x'",
      pg_constraint: "A-B",
      pg_routine: "exec stmt",
      pg_where_frame: "PL/pgSQL function public.org_secret() line 1 at RAISE",
      pg_message_class: "publication: user fence missing",
      sqs_message_id: "id with spaces",
      sqs_receive_count: "3",
      sqs_message_count: -1,
    });
    for (const k of ["pg_table", "pg_constraint", "pg_routine", "pg_where_frame", "pg_message_class", "sqs_message_id", "sqs_receive_count", "sqs_message_count"]) expect(out).not.toHaveProperty(k);
    const ok = line({
      pg_table: "session_index",
      pg_constraint: "session_index_pkey",
      pg_routine: "exec_stmt_raise",
      pg_where_frame: "PL/pgSQL function other(...) line 1 at RAISE",
      pg_message_class: "pg_deadlock_detected",
      sqs_message_id: "059f36b4-87a3-44ab-83d2-661975830a7d",
      sqs_receive_count: 3,
      sqs_message_count: 1,
    });
    expect(ok).toMatchObject({
      pg_table: "session_index",
      pg_constraint: "session_index_pkey",
      pg_routine: "exec_stmt_raise",
      pg_where_frame: "PL/pgSQL function other(...) line 1 at RAISE",
      pg_message_class: "pg_deadlock_detected",
      sqs_message_id: "059f36b4-87a3-44ab-83d2-661975830a7d",
      sqs_receive_count: 3,
      sqs_message_count: 1,
    });
  });
});
