import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { SESSION_BOOTSTRAP_FILES } from "../session-bootstrap-chain.ts";
import { SESSION_BOOTSTRAP_ROLES } from "../session-bootstrap-roles.ts";
import { chainFileRoles } from "../session-bootstrap-consumers.ts";

const root = new URL("../../../", import.meta.url);
const sql = (file: string) =>
  readFileSync(new URL(file, root), "utf8")
    .split("\n")
    .filter((l) => !/^\s*--/.test(l))
    .join("\n");
/** The extractor the bootstrap stage applies to the anchor's bytes (one source). */
const provisioned = chainFileRoles;

test("SESSION_BOOTSTRAP_ROLES covers exactly the chain files, in chain order", () => {
  expect(Object.keys(SESSION_BOOTSTRAP_ROLES)).toEqual([...SESSION_BOOTSTRAP_FILES]);
  for (const file of SESSION_BOOTSTRAP_FILES)
    expect(Object.keys(SESSION_BOOTSTRAP_ROLES[file]).length).toBeGreaterThan(0);
});

test("every listed role literally appears in its file and no file provisions an unlisted role", () => {
  const seen = new Set<string>();
  for (const file of SESSION_BOOTSTRAP_FILES) {
    const text = sql(file);
    const listed = Object.keys(SESSION_BOOTSTRAP_ROLES[file]);
    for (const name of listed) {
      expect(text).toMatch(new RegExp(`(^|[^a-z0-9_])${name}([^a-z0-9_]|$)`));
      // A role belongs to one file only.
      expect(seen.has(name)).toBe(false);
      seen.add(name);
    }
    expect([...provisioned(text)].sort()).toEqual([...listed].sort());
    // Every CREATE ROLE is a literal name or a format(%I) over v_name/v_role.
    const creates = [...text.matchAll(/CREATE ROLE\s+(\S+)/g)].map((m) => m[1]);
    expect(creates.length).toBeGreaterThan(0);
    for (const c of creates) expect(c === "%I" || /^[a-z_][a-z0-9_]*$/.test(c)).toBe(true);
    if (creates.includes("%I"))
      expect(text).toMatch(/format\('CREATE ROLE %I[^']*', v_(?:name|role)\)/);
  }
});

test("the extractor itself catches an unlisted role in a chain file", () => {
  const text = sql(SESSION_BOOTSTRAP_FILES[3]) + "\nDO $x$ DECLARE v_name text := 'rogue_role'; BEGIN END $x$;";
  expect(provisioned(text).has("rogue_role")).toBe(true);
  expect(provisioned("CREATE ROLE sneaky NOLOGIN;").has("sneaky")).toBe(true);
  expect(
    provisioned("FOREACH v_name IN ARRAY ARRAY['a_role', 'b_role'] LOOP"),
  ).toEqual(new Set(["a_role", "b_role"]));
});

test("contracts: only non-bare roles declare login, inbound members or settings", () => {
  const all = Object.assign({}, ...Object.values(SESSION_BOOTSTRAP_ROLES));
  expect(all.session_directory_search_worker).toEqual({ login: "never", inbound: "worker", settings: "worker-2s" });
  expect(all.session_token_verifier).toEqual({ login: "never", inbound: "verifier", settings: "unchecked" });
  expect(all.bulls96_session_ingest_seeder.login).toBe("never");
  expect(all.bulls96_read_api.inbound).toBe("none");
  expect(all.bulls96_acquisition_installer.inbound).toBe("none");
  expect(all.buffered_search_feed_runtime.inbound).toBe("none");
  expect(all.bulls96_analysis_admit).toEqual({ login: "allowed", inbound: "none", settings: "unchecked" });
  expect(all.bulls96_digest_narrative).toEqual({ login: "allowed", inbound: "none", settings: "unchecked" });
  expect(all.search_consumer_runtime).toEqual({ login: "claimed-60s", inbound: "consumer", settings: "consumer" });
});

// The PR schema rehearsal (scripts/pg-proof/schema-rehearsal.ts) pins the WHOLE text of
// the chain module at the release sha to the landing commit's bytes (refusing
// rehearsal_seed_bootstrap_chain otherwise). So the role contract map lives in
// session-bootstrap-roles.ts, and the chain module must change only when the chain file
// list does. If the driver's pinned path ever moves, this test must move with it.
test("the rehearsal-pinned chain module carries no role contract map", () => {
  const chainModule = "src/hoopdb/session-bootstrap-chain.ts";
  expect(readFileSync(new URL(chainModule, root), "utf8")).not.toMatch(/\bSESSION_BOOTSTRAP_ROLES\b/);
  const driver = readFileSync(new URL("scripts/pg-proof/schema-rehearsal.ts", root), "utf8");
  expect(driver).toContain(`const BOOTSTRAP_CHAIN = "${chainModule}";`);
});
