/**
 * Pure unit tests for the read-only production database checks: the fixed query set, the artifact
 * sanitizer, the runner's refusals and window ordering against a fake query function, and the
 * workflow/role-SQL text. Nothing here opens a database connection.
 */
import { describe, expect, test } from "bun:test";
import { existsSync, readFileSync } from "node:fs";
import {
  allStatements,
  EXPLAIN,
  NAMED_FUNCTIONS,
  NAMED_INDEXES,
  NAMED_TABLES,
  PRECHECK,
  READONLY_ROLE,
  SAMPLE,
  executedStatements,
  sessionOptions,
  SNAPSHOT,
  SYNTHETIC,
  TABLE_DATA_PENDING_COUNT_FUNCTION,
  TABLE_DATA_STATUS,
} from "../../../scripts/db-readonly-checks-queries.ts";
import { sanitizeArtifact, SanitizeError } from "../../../scripts/db-readonly-checks-sanitize.ts";
import {
  artifactFailed,
  parseInputs,
  readonlyClientConfig,
  runChecks,
} from "../../../scripts/db-readonly-checks.ts";
import { environmentGate, GateRefusal } from "../../../scripts/db-readonly-environment-gate.ts";

const root = new URL("../../../", import.meta.url);
const read = (p: string) => readFileSync(new URL(p, root), "utf8");
/** Removes single-quoted SQL literals so keyword checks see only the statement structure. */
const structure = (sql: string) => sql.replace(/'(?:[^']|'')*'/g, "''");

// Built at runtime so no identifier-shaped literal appears in the source.
const UUIDISH = ["a".repeat(8), "b".repeat(4), "c".repeat(4), "d".repeat(4), "e".repeat(12)].join("-");
const HEX32 = "0f".repeat(16);
const JWTISH = ["eyJ" + "h".repeat(12), "p".repeat(12), "s".repeat(12)].join(".");
const REFISH = "q".repeat(12) + "7".repeat(8);

describe("query set shape", () => {
  const all = allStatements();

  test("every statement is a single static SELECT, EXPLAIN or SHOW", () => {
    expect(all.length).toBeGreaterThan(20);
    for (const s of all) {
      expect(typeof s.sql).toBe("string");
      expect(s.sql).toMatch(/^(SELECT|EXPLAIN|SHOW) /);
      expect(s.sql).not.toContain(";");
      expect(s.sql).not.toContain("--");
      expect(s.sql).not.toContain("/*");
    }
  });

  test("no write, DDL, session-changing or server-side-effect keywords", () => {
    const banned =
      /\b(INSERT|UPDATE|DELETE|TRUNCATE|MERGE|ALTER|CREATE|DROP|GRANT|REVOKE|COPY|CALL|DO|SET|RESET|LOCK|VACUUM|ANALY[SZ]E|LISTEN|NOTIFY|PREPARE|EXECUTE|DISCARD|COMMIT|ROLLBACK|BEGIN)\b|set_config|pg_terminate_backend|pg_cancel_backend|pg_stat_statements_reset|pg_stat_reset|nextval|setval|dblink|lo_|pg_read_(binary_)?file|pg_ls_|pg_sleep|backfill_cursor/i;
    for (const s of all) expect({ name: s.name, hit: structure(s.sql).match(banned)?.[0] ?? null }).toEqual({ name: s.name, hit: null });
  });

  test("$ placeholders appear only in EXPLAIN shapes, contiguous and bound to synthetic values", () => {
    const synthetic = new Set<string>(Object.values(SYNTHETIC));
    for (const s of all) {
      const marks = [...structure(s.sql).matchAll(/\$([0-9]+)/g)].map((m) => Number(m[1]));
      if (!s.name.startsWith("explain.")) {
        expect({ name: s.name, marks }).toEqual({ name: s.name, marks: [] });
        expect(s.params).toEqual([]);
        continue;
      }
      const max = Math.max(...marks);
      expect(new Set(marks)).toEqual(new Set(Array.from({ length: max }, (_, i) => i + 1)));
      expect(s.params.length).toBe(max);
      for (const p of s.params) expect(synthetic.has(p)).toBe(true);
    }
  });

  test("EXPLAIN is plan-only JSON, never ANALYZE, BUFFERS or SETTINGS", () => {
    for (const [name, shape] of Object.entries(EXPLAIN)) {
      expect({ name, ok: shape.sql.startsWith("EXPLAIN (FORMAT JSON, COSTS true) SELECT ") }).toEqual({ name, ok: true });
      expect(shape.sql).not.toMatch(/ANALY[SZ]E|BUFFERS|SETTINGS|WAL|TIMING/i);
    }
    expect(Object.keys(EXPLAIN).sort()).toEqual([
      "reader_detail_044040_gin",
      "reader_detail_v2",
      "reader_list_outbox",
      "reader_list_v2",
      "writer_batch_committed_lookup",
      "writer_refresh_claims",
    ]);
  });

  test("executed statements read only statistics/catalog views, never a sessions table", () => {
    const executed = executedStatements();
    expect(executed.length).toBeGreaterThan(10);
    for (const s of executed) {
      expect({ name: s.name, table: structure(s.sql).match(/\bsessions\.[a-z_]+/)?.[0] ?? null }).toEqual({ name: s.name, table: null });
      expect(s.sql).not.toMatch(/^EXPLAIN/);
    }
    // Every table-data statement lives in the not-executed specification.
    const spec = new Set(allStatements().filter((s) => /\bsessions\./.test(structure(s.sql))).map((s) => s.name.split(".")[0]));
    expect([...spec].sort()).toEqual(["explain", "policy", "reconcile"]);
    expect(Object.keys(TABLE_DATA_PENDING_COUNT_FUNCTION).sort()).toEqual(["explain", "policy", "reconcile"]);
    expect(PRECHECK.schemaSelectable).toContain("has_table_privilege(c.oid, 'SELECT')");
  });

  test("named lists are identifiers and table reads stay inside NAMED_TABLES", () => {
    for (const n of [...NAMED_TABLES, ...NAMED_INDEXES, ...NAMED_FUNCTIONS]) expect(n).toMatch(/^[a-z_][a-z0-9_]*$/);
    const tables = new Set<string>(NAMED_TABLES);
    for (const s of all)
      for (const m of structure(s.sql).matchAll(/\bsessions\.([a-z_]+)/g))
        expect({ name: s.name, table: m[1], named: tables.has(m[1]!) }).toEqual({ name: s.name, table: m[1], named: true });
  });

  test("session options pin read-only, lock_timeout 2s and the chosen statement_timeout", () => {
    expect(sessionOptions(15)).toBe(
      "-c default_transaction_read_only=on -c statement_timeout=15000 -c lock_timeout=2000 -c idle_in_transaction_session_timeout=60000",
    );
    expect(sessionOptions(60)).toContain("-c statement_timeout=60000 -c lock_timeout=2000 ");
    for (const bad of [4, 61, 7.5, Number.NaN]) expect(() => sessionOptions(bad)).toThrow("statement_timeout_range");
  });
});

describe("sanitizer", () => {
  const refused = (v: unknown, secrets: string[] = []) => {
    try {
      sanitizeArtifact(v, secrets);
      return null;
    } catch (e) {
      expect(e).toBeInstanceOf(SanitizeError);
      return (e as SanitizeError).code;
    }
  };

  test("refuses identifier and credential shapes in values and keys", () => {
    expect(refused({ a: UUIDISH })).toBe("uuid");
    expect(refused({ a: UUIDISH.toUpperCase() })).toBe("uuid");
    expect(refused({ a: UUIDISH.replaceAll("-", "").toUpperCase() })).toBe("hex32");
    expect(refused({ a: `x${HEX32}y` })).toBe("hex32");
    expect(refused({ a: JWTISH })).toBe("jwt");
    expect(refused({ a: "see postgresql://u@h/d" })).toBe("url");
    expect(refused({ a: "https://example.invalid/x" })).toBe("url");
    expect(refused({ a: "someone@example.org" })).toBe("email");
    expect(refused({ a: "db.example.supabase.co" })).toBe("supabase_host");
    expect(refused({ a: "aws-0-x.pooler.example" })).toBe("supabase_host");
    expect(refused({ a: REFISH })).toBe("project_ref_shape");
    expect(refused({ [UUIDISH]: 1 })).toBe("uuid");
    expect(refused({ nested: [{ deeper: [HEX32] }] })).toBe("hex32");
  });

  test("refuses secret fragments, odd types, oversized strings", () => {
    expect(refused({ a: "prefix-S3cretValue-suffix" }, ["S3cretValue"])).toBe("secret_fragment");
    expect(refused({ a: new Date(0) })).toBe("unsupported_type");
    expect(refused({ a: Number.NaN })).toBe("non_finite");
    expect(refused({ a: "x".repeat(300) })).toBe("string_too_long");
  });

  test("the refusal message names the path, never the value", () => {
    try {
      sanitizeArtifact({ outer: { v: UUIDISH } });
      throw new Error("expected refusal");
    } catch (e) {
      expect((e as Error).message).toBe("artifact refused: uuid at $.outer.v");
      expect((e as Error).message).not.toContain(UUIDISH);
    }
  });

  test("accepts the catalog, timestamp and planner vocabulary the artifact uses", () => {
    const ok = {
      at: "2026-11-11T05:00:00.123Z",
      index: "ingest_claim_sessions_accepted_idx",
      gin: "ingest_claim_attempts_records_gin_idx",
      node: "Index Only Scan",
      wait: { wait_event_type: "Client", wait_event: "ClientRead", state: "idle in transaction" },
      lock: { locktype: "transactionid", mode: "ShareUpdateExclusiveLock", granted: true },
      version: "1.10",
      n: 12345678901234,
      fns: [...NAMED_FUNCTIONS],
    };
    expect(refused(ok)).toBeNull();
  });
});

describe("inputs and connection config", () => {
  test("interval is bounded, explain is a strict boolean, output is a plain file name", () => {
    expect(parseInputs({})).toEqual({ intervalSeconds: 900, statementTimeoutSeconds: 15, explain: false, output: "db-readonly-checks.json" });
    expect(parseInputs({ STATEMENT_TIMEOUT_SECONDS: "5" }).statementTimeoutSeconds).toBe(5);
    expect(parseInputs({ STATEMENT_TIMEOUT_SECONDS: "60" }).statementTimeoutSeconds).toBe(60);
    for (const bad of ["4", "61", "0", "-5", "15.5", "1e1", "100", " 15", "abc"])
      expect(() => parseInputs({ STATEMENT_TIMEOUT_SECONDS: bad })).toThrow("statement_timeout_seconds_invalid");
    expect(parseInputs({ INTERVAL_SECONDS: "60", EXPLAIN: "true" }).intervalSeconds).toBe(60);
    expect(parseInputs({ INTERVAL_SECONDS: "1800" }).intervalSeconds).toBe(1800);
    for (const bad of ["59", "1801", "-1", "1e3", "900 ", "abc", "0x10"])
      expect(() => parseInputs({ INTERVAL_SECONDS: bad })).toThrow("interval_seconds_invalid");
    expect(() => parseInputs({ EXPLAIN: "yes" })).toThrow("explain_invalid");
    expect(() => parseInputs({ OUTPUT: "../x.json" })).toThrow("output_invalid");
    expect(() => parseInputs({ OUTPUT: "/tmp/x.json" })).toThrow("output_invalid");
  });

  const ref = "r".repeat(20);
  const url = (user: string, q = "?sslmode=verify-full") => `postgresql://${user}:pw-not-real@db-host.example.invalid:6543/postgres${q}`;

  test("discrete fields, pinned TLS, read-only startup options, exact role", () => {
    const c = readonlyClientConfig(url(`${READONLY_ROLE}.${ref}`), ref);
    expect(c).not.toHaveProperty("connectionString");
    expect(c.user).toBe(`${READONLY_ROLE}.${ref}`);
    expect(c.port).toBe(6543);
    expect(c.options).toBe(sessionOptions(15));
    expect(c.query_timeout).toBe(30000);
    const c60 = readonlyClientConfig(url(READONLY_ROLE), ref, 60);
    expect(c60.options).toBe(sessionOptions(60));
    expect(c60.query_timeout).toBe(75000);
    expect((c.ssl as { rejectUnauthorized: boolean }).rejectUnauthorized).toBe(true);
    expect(readonlyClientConfig(url(READONLY_ROLE), ref).user).toBe(READONLY_ROLE);
  });

  test("refuses another role, another ref suffix, a missing URL and weakened TLS", () => {
    expect(() => readonlyClientConfig(url("postgres"), ref)).toThrow("db_readonly_user");
    expect(() => readonlyClientConfig(url(`${READONLY_ROLE}.${"s".repeat(20)}`), ref)).toThrow("db_readonly_user");
    expect(() => readonlyClientConfig(undefined, ref)).toThrow("db_readonly_url_required");
    expect(() => readonlyClientConfig(url(READONLY_ROLE, "?sslmode=disable"), ref)).toThrow("db_readonly_url_query_params");
  });
});

describe("runner against a fake query function", () => {
  const allowed = new Set(executedStatements().map((s) => s.sql));
  type Row = Record<string, unknown>;
  function fake(overrides: Partial<Record<string, Row[]>> = {}) {
    const log: string[] = [];
    const answers: Record<string, Row[]> = {
      [PRECHECK.readOnly]: [{ transaction_read_only: "on" }],
      [PRECHECK.defaultReadOnly]: [{ default_transaction_read_only: "on" }],
      [PRECHECK.statementTimeout]: [{ statement_timeout: "15s" }],
      [PRECHECK.lockTimeout]: [{ lock_timeout: "2s" }],
      [PRECHECK.serverVersion]: [{ server_version_num: "170004" }],
      [PRECHECK.identity]: [
        { me: READONLY_ROLE, session: READONLY_ROLE, rolsuper: false, rolcreaterole: false, rolcreatedb: false, rolbypassrls: false, rolreplication: false, read_all_stats: true },
      ],
      [PRECHECK.memberships]: [{ roles: ["pg_monitor", "pg_read_all_stats"] }],
      [PRECHECK.schemaWritable]: [{ writable: 0 }],
      [PRECHECK.schemaSelectable]: [{ selectable: 0 }],
      [SNAPSHOT.statsReset]: [{ stats_reset: new Date(0), at: new Date(1000) }],
      [SNAPSHOT.pgssLocation]: [{ schema: "extensions", version: "1.10" }],
      [SNAPSHOT.pgssTrack]: [{ "pg_stat_statements.track": "top" }],
      [SAMPLE.waits]: [{ wait_event_type: "Client", wait_event: "ClientRead", state: "idle", n: 3, blocked: 0 }],
      [SAMPLE.locks]: [{ locktype: "relation", mode: "AccessShareLock", granted: true, relname: "ingest_claim_sessions", n: 2 }],
      ...overrides,
    };
    let idx = 0;
    const q = async (sql: string, params?: readonly string[]) => {
      if (!allowed.has(sql)) throw new Error("statement outside the executed set");
      log.push(sql);
      if (sql in answers) return answers[sql]!;
      if (sql.startsWith("SELECT s.relname, s.indexrelname")) {
        idx += 1;
        return NAMED_INDEXES.map((n) => ({ relname: "t", indexrelname: n, idx_scan: String(idx * 10), idx_tup_read: "5", idx_tup_fetch: "5", valid: true, at: new Date(idx * 1000) }));
      }
      if (sql.startsWith("SELECT f.fn")) return NAMED_FUNCTIONS.map((fn) => ({ fn, entries: 1, calls: String(idx * 7), total_exec_ms: 1.5, max_exec_ms: 1, rows: "1", shared_blks_hit: "1", shared_blks_read: "0", temp_blks_written: "0", at: new Date(0) }));
      if (sql.startsWith("SELECT i.dealloc")) return [{ dealloc: "0", stats_reset: new Date(0) }];
      return [{ n: "0" }];
    };
    return { q, log };
  }
  const sleeps: number[] = [];
  const sleep = async (ms: number) => {
    sleeps.push(ms);
  };

  test("happy path: sanitized artifact, deltas, sleeps sum to the interval, no table reads in the window", async () => {
    sleeps.length = 0;
    const { q, log } = fake();
    const a = await runChecks(q, { intervalSeconds: 75, statementTimeoutSeconds: 15, explain: true, output: "x.json" }, sleep);
    expect(() => sanitizeArtifact(a, ["pw-not-real"])).not.toThrow();
    expect(artifactFailed(a)).toBe(false);
    expect(sleeps.reduce((x, y) => x + y, 0)).toBe(75_000);
    expect(a.activity.samples).toBe(3);
    expect(a.index_usage.rows.every((r) => r.idx_scan_delta === 10)).toBe(true);
    expect(a.session).toEqual({ statement_timeout_seconds: 15, lock_timeout_seconds: 2, read_only: true });
    expect(a.role.table_select_privilege).toBe(false);
    // Table data is skipped with an explicit status, never run and never reported as zero.
    expect(a.table_data.status).toBe(TABLE_DATA_STATUS);
    expect(TABLE_DATA_STATUS).toBe("not_permitted_pending_count_function");
    expect(a.table_data.skipped.reconcile).toEqual(["pairs", "stale", "nullAccepted", "drifted"]);
    expect(a.table_data.skipped.explain).toHaveLength(6);
    expect(JSON.stringify(a)).not.toMatch(/expected_pairs|stale_rows|write_enabled/);
    expect(log.some((s) => /\bsessions\.ingest|^EXPLAIN/.test(s))).toBe(false);
  });

  test("statement_timeout input flows into the precheck: a mismatched SHOW refuses", async () => {
    const { q } = fake({ [PRECHECK.statementTimeout]: [{ statement_timeout: "1min" }] });
    const a = await runChecks(q, { intervalSeconds: 60, statementTimeoutSeconds: 60, explain: false, output: "x.json" }, sleep);
    expect(a.session.statement_timeout_seconds).toBe(60);
    expect(a.table_data.skipped.explain).toEqual([]);
    expect(artifactFailed(a)).toBe(false);
    await expect(runChecks(fake().q, { intervalSeconds: 60, statementTimeoutSeconds: 30, explain: false, output: "x.json" }, sleep)).rejects.toThrow(
      "refused: statement_timeout",
    );
  });

  const refusals: [string, Partial<Record<string, Row[]>>, string][] = [
    ["not read-only", { [PRECHECK.readOnly]: [{ transaction_read_only: "off" }] }, "transaction_not_read_only"],
    ["timeout drift", { [PRECHECK.statementTimeout]: [{ statement_timeout: "0" }] }, "statement_timeout"],
    ["other login", { [PRECHECK.identity]: [{ me: "postgres", session: "postgres", rolsuper: false, rolcreaterole: false, rolcreatedb: false, rolbypassrls: false, rolreplication: false, read_all_stats: true }] }, "identity"],
    ["bypassrls", { [PRECHECK.identity]: [{ me: READONLY_ROLE, session: READONLY_ROLE, rolsuper: false, rolcreaterole: false, rolcreatedb: false, rolbypassrls: true, rolreplication: false, read_all_stats: true }] }, "identity"],
    ["no stats role", { [PRECHECK.identity]: [{ me: READONLY_ROLE, session: READONLY_ROLE, rolsuper: false, rolcreaterole: false, rolcreatedb: false, rolbypassrls: false, rolreplication: false, read_all_stats: false }] }, "pg_read_all_stats_missing"],
    ["extra membership", { [PRECHECK.memberships]: [{ roles: ["pg_monitor", "pg_write_all_data"] }] }, "memberships"],
    ["write grant", { [PRECHECK.schemaWritable]: [{ writable: 1 }] }, "write_privilege_present"],
    ["any table SELECT grant", { [PRECHECK.schemaSelectable]: [{ selectable: 1 }] }, "table_privilege_present"],
    ["unanswered privilege check", { [PRECHECK.schemaSelectable]: [] }, "table_privilege_present"],
  ];
  for (const [name, override, code] of refusals)
    test(`refuses: ${name}`, async () => {
      const { q, log } = fake(override);
      await expect(runChecks(q, { intervalSeconds: 60, statementTimeoutSeconds: 15, explain: true, output: "x.json" }, sleep)).rejects.toThrow(`refused: ${code}`);
      expect(log.some((s) => /\bsessions\.ingest/.test(s))).toBe(false);
    });

});

describe("workflow and role SQL text", () => {
  const wf = read(".github/workflows/db-readonly-checks.yml");
  const role = read("scripts/sql/db-readonly-role.sql");
  const login = read("scripts/sql/db-readonly-role-login.sql.tmpl");

  test("workflow: dispatch only, main only, gated environment, secret only in the run step", () => {
    expect(wf).toMatch(/^on:\n  workflow_dispatch:\n/m);
    expect(wf).not.toMatch(/^\s+(push|pull_request|pull_request_target|schedule|workflow_run|repository_dispatch):/m);
    expect(wf).toContain("github.ref == 'refs/heads/main'");
    expect(wf).toContain("environment: production-db-readonly");
    expect(wf.match(/secrets\.DB_READONLY_URL/g)?.length).toBe(1);
    // "never ANALYZE" may appear in comments and the input description; nowhere else.
    expect(wf.replace(/^\s*#.*$|^\s*description:.*$/gm, "")).not.toMatch(/ANALY[SZ]E/);
    expect(wf).toContain("bun scripts/db-readonly-checks.ts");
    expect(wf).toMatch(/^permissions:\n  contents: read\n  actions: read\nconcurrency:/m);
    // The gate job has no environment and runs the protection check before the gated job.
    expect(wf).toMatch(/  gate:\n    runs-on: ubuntu-latest\n/);
    expect(wf).toContain("bun scripts/db-readonly-environment-gate.ts production-db-readonly\n");
    expect(wf).toMatch(/  checks:\n    needs: gate\n/);
    expect(wf.match(/environment: /g)?.length).toBe(1);
  });

  test("only the checks workflow exists: no login workflow, no admin environment, no secret writer", () => {
    expect(existsSync(new URL(".github/workflows/db-readonly-role-login.yml", root))).toBe(false);
    expect(existsSync(new URL("scripts/db-readonly-role-login.ts", root))).toBe(false);
    for (const s of ["DB_ROLE_ADMIN_URL", "DB_READONLY_SECRET_WRITER_TOKEN", "production-db-readonly-admin"]) expect(wf).not.toContain(s);
  });

  test("workflow: statement_timeout_seconds input, validated 5..60 in the gate job and passed to the runner", () => {
    expect(wf).toMatch(/      statement_timeout_seconds:\n[^]*?type: number\n[^]*?default: 15\n/);
    expect(wf).toContain("STATEMENT_TIMEOUT_SECONDS: ${{ inputs.statement_timeout_seconds }}");
    expect(wf).toContain("^[0-9]{1,2}$");
    expect(wf).toMatch(/STATEMENT_TIMEOUT_SECONDS" -lt 5/);
    expect(wf).toMatch(/STATEMENT_TIMEOUT_SECONDS" -gt 60/);
  });

  test("role SQL: NOLOGIN, read-only defaults, statistics memberships only, no table access, no policies", () => {
    const body = role.replace(/--.*$/gm, "");
    expect(body).toContain(`CREATE ROLE ${READONLY_ROLE} NOLOGIN`);
    expect(body).not.toMatch(/(?<!NO)LOGIN\b/);
    expect(body).not.toMatch(/(?<!NO)(BYPASSRLS|SUPERUSER|CREATEROLE|CREATEDB|REPLICATION)\b|\bPASSWORD\b/);
    expect(body).toMatch(/ALTER ROLE sessions_readonly_stats SET default_transaction_read_only = on;/);
    expect(body).not.toMatch(/GRANT\s+(INSERT|UPDATE|DELETE|TRUNCATE|ALL|REFERENCES|TRIGGER|CREATE|EXECUTE)/i);
    expect(body).not.toMatch(/FOR\s+(ALL|INSERT|UPDATE|DELETE)\b/i);
    expect(body).not.toContain("backfill_cursor");
    expect(body).not.toMatch(/GRANT\s+SELECT/i);
    expect(body).not.toMatch(/CREATE\s+POLICY/i);
    expect(body).not.toMatch(/GRANT\s+USAGE\s+ON\s+SCHEMA\s+sessions\b/i);
    expect(body).toMatch(/GRANT pg_read_all_stats TO sessions_readonly_stats;/);
    expect(body).toMatch(/GRANT pg_monitor TO sessions_readonly_stats;/);
    expect(body).toContain("has_table_privilege(v_role, c.oid, 'SELECT')");
    const nologin = read("scripts/sql/db-readonly-role-nologin.sql").replace(/--.*$/gm, "");
    expect(nologin).toContain("NOLOGIN PASSWORD NULL");
    expect(nologin).not.toMatch(/\bGRANT\b/);
  });

  test("login template: one psql verifier variable, no literal password", () => {
    expect(login.match(/:'login_verifier'/g)?.length).toBe(1);
    const body = login.replace(/--.*$/gm, "");
    expect(body).toMatch(/ALTER ROLE sessions_readonly_stats WITH LOGIN PASSWORD :'login_verifier';/);
    expect(body).not.toMatch(/PASSWORD\s+'/);
    expect(login).not.toMatch(/db-readonly-role-login\.(ts|yml)/);
  });
});

describe("environment protection gate (fake GitHub API)", () => {
  const ctx = { GITHUB_REPOSITORY: "example-org/example-repo", GITHUB_EVENT_NAME: "workflow_dispatch", GITHUB_REF: "refs/heads/main" };
  const protectedEnv = (name: string) => ({
    name,
    protection_rules: [{ type: "required_reviewers", reviewers: [{ type: "User", reviewer: { type: "User", login: "owner" } }] }],
    deployment_branch_policy: { protected_branches: false, custom_branch_policies: true },
  });
  const mainOnly = { total_count: 1, branch_policies: [{ name: "main", type: "branch" }] };
  const fake = (env: object, branches: object) => async (path: string) => (path.includes("deployment-branch-policies") ? branches : env) as Record<string, any>;
  const code = async (p: Promise<unknown>) => {
    try {
      await p;
      return "ok";
    } catch (e) {
      return e instanceof GateRefusal ? e.code : "other";
    }
  };

  test("accepts a protected main-only environment and asks only for it", async () => {
    const paths: string[] = [];
    const get = async (path: string) => {
      paths.push(path);
      return fake(protectedEnv("production-db-readonly"), mainOnly)(path);
    };
    expect(await code(environmentGate(ctx, "production-db-readonly", get))).toBe("ok");
    expect(paths).toEqual([
      "/repos/example-org/example-repo/environments/production-db-readonly",
      "/repos/example-org/example-repo/environments/production-db-readonly/deployment-branch-policies?per_page=100",
    ]);
  });

  test("refuses unknown names, wrong context, missing reviewers, open branches, extra branch policies", async () => {
    const ok = fake(protectedEnv("production-db-readonly"), mainOnly);
    expect(await code(environmentGate(ctx, "production", ok))).toBe("environment_name");
    expect(await code(environmentGate(ctx, "production-db-readonly-admin", ok))).toBe("environment_name");
    expect(await code(environmentGate({ ...ctx, GITHUB_REF: "refs/heads/x" }, "production-db-readonly", ok))).toBe("context");
    expect(await code(environmentGate({ ...ctx, GITHUB_EVENT_NAME: "push" }, "production-db-readonly", ok))).toBe("context");
    expect(await code(environmentGate({ ...ctx, GITHUB_REPOSITORY: "a/b/../c" }, "production-db-readonly", ok))).toBe("context");
    const noReviewers = { ...protectedEnv("production-db-readonly"), protection_rules: [] };
    expect(await code(environmentGate(ctx, "production-db-readonly", fake(noReviewers, mainOnly)))).toBe("environment_protection");
    const teamOnly = { ...protectedEnv("production-db-readonly"), protection_rules: [{ type: "required_reviewers", reviewers: [{ type: "Team", reviewer: {} }] }] };
    expect(await code(environmentGate(ctx, "production-db-readonly", fake(teamOnly, mainOnly)))).toBe("environment_protection");
    const allBranches = { ...protectedEnv("production-db-readonly"), deployment_branch_policy: null };
    expect(await code(environmentGate(ctx, "production-db-readonly", fake(allBranches, mainOnly)))).toBe("environment_protection");
    const extra = { total_count: 2, branch_policies: [{ name: "main", type: "branch" }, { name: "*", type: "branch" }] };
    expect(await code(environmentGate(ctx, "production-db-readonly", fake(protectedEnv("production-db-readonly"), extra)))).toBe("environment_branch");
    const tag = { total_count: 1, branch_policies: [{ name: "main", type: "tag" }] };
    expect(await code(environmentGate(ctx, "production-db-readonly", fake(protectedEnv("production-db-readonly"), tag)))).toBe("environment_branch");
  });
});
