import { Database } from "bun:sqlite";
import { test, expect } from "bun:test";
import {
  mkdtemp,
  writeFile,
  readFile,
  rm,
  stat,
  readdir,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  exportLocalWorkspace,
  writeLocalExport,
  type LocalExportOptions,
} from "../src/local-export.ts";

async function fixture() {
  const directory = await mkdtemp(join(tmpdir(), "coordination-local-"));
  const dbPath = join(directory, "history.db");
  const db = new Database(dbPath);
  db.exec(`
    CREATE TABLE delivery_jobs(id TEXT PRIMARY KEY,state TEXT);
    CREATE TABLE change_feed_store(epoch INTEGER,export_schema_digest TEXT);
    CREATE TABLE delivery_exclusions(source TEXT,session_id TEXT);
    CREATE TABLE sessions(source TEXT,session_id TEXT,title TEXT,project_key TEXT,cwd TEXT,discovery_state TEXT,revision INTEGER,last_activity_ms INTEGER,first_activity_ms INTEGER,project_key_method TEXT);
    CREATE TABLE session_events(id INTEGER PRIMARY KEY,source TEXT,session_id TEXT,ts_ms INTEGER,role TEXT,kind TEXT,text TEXT,is_meta INTEGER,control_kind TEXT,location TEXT,event_uid TEXT,payload_truncated INTEGER,revision INTEGER);
    CREATE TABLE file_edits(id INTEGER PRIMARY KEY,source TEXT,session_id TEXT,ts_ms INTEGER,location TEXT,tool_use_id TEXT,file_path TEXT,cwd TEXT,tool_name TEXT,revision INTEGER);
    CREATE TABLE session_relationships(source TEXT,parent_session_id TEXT,child_session_id TEXT,relationship TEXT);
    INSERT INTO delivery_jobs VALUES('job','active');
    INSERT INTO change_feed_store VALUES(123,'schema-v1');
  `);
  const start = Date.parse("2026-11-01T00:00:00.000Z");
  for (const [id, project] of [
    ["allowed", "github.com/Example/repo"],
    ["excluded", "github.com/Example/repo"],
    ["foreign", "github.com/Other/repo"],
  ]) {
    db.query(
      "INSERT INTO sessions VALUES('codex',?,?,?,'/repo','full',1,?,?,'remote')",
    ).run(id!, "Contact alex@example.com", project!, start + 30000, start);
    for (let i = 0; i < 10; i++)
      db.query(
        "INSERT INTO session_events VALUES(NULL,'codex',?,?,'assistant','text',?,0,NULL,'local',?,0,1)",
      ).run(
        id!,
        start + i * 1000,
        `Message ${i}. Contact alex@example.com.`,
        `event-${i}`,
      );
  }
  db.exec("INSERT INTO delivery_exclusions VALUES('codex','excluded')");
  db.query(
    "INSERT INTO file_edits VALUES(1,'codex','allowed',?,'local','edit-1','/repo/src/api.ts','/repo','Edit',2)",
  ).run(start + 15000);
  db.close();
  await writeFile(
    join(directory, "config.json"),
    JSON.stringify({
      version: 1,
      site_url: "https://agentrelay.com",
      org_id: "org",
      workspace_id: "workspace",
      job_id: "job",
      sharing_mode: "new",
    }),
    { mode: 0o600 },
  );
  const options: LocalExportOptions = {
    dbPath,
    organizationId: "org",
    workspaceId: "workspace",
    projectPrefix: "github.com/Example/",
    machineLabel: "Fixture machine",
    since: "2026-11-01T00:00:00.000Z",
    until: "2026-11-02T00:00:00.000Z",
    exportedAt: "2026-11-03T00:00:00.000Z",
    perSessionLimit: 5,
    fileEditLimit: 1,
  };
  return { directory, options };
}

test("local export uses verified scope, sharing exclusions and read-only normalized evidence", async () => {
  const { directory, options } = await fixture();
  try {
    const before = await readFile(options.dbPath);
    const result = await exportLocalWorkspace(options);
    expect(result.exported.sessions.map((s) => s.sessionId)).toEqual([
      "allowed",
    ]);
    expect(result.exported.events).toHaveLength(5);
    expect(result.inventory.omittedConversations).toBe(6);
    expect(result.exported.events.every((e) => e.availableAt === null)).toBe(
      true,
    );
    expect(
      result.exported.events.find((e) => e.kind === "file_edit")!.filesTouched,
    ).toEqual(["/repo/src/api.ts"]);
    expect(
      result.exported.events
        .filter((e) => e.kind === "text")
        .every((e) => e.filesTouched.length === 0),
    ).toBe(true);
    expect(JSON.stringify(result)).not.toContain("alex@example.com");
    expect(result.exported.coverage.status).toBe("partial");
    expect(result.exported.coverage.notes).toContain("not team-wide");
    expect(await readFile(options.dbPath)).toEqual(before);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("local export refuses mismatched scopes and unsupported sharing modes", async () => {
  const { directory, options } = await fixture();
  try {
    await expect(
      exportLocalWorkspace({ ...options, workspaceId: "foreign" }),
    ).rejects.toThrow("does not match");
    await writeFile(
      join(directory, "config.json"),
      JSON.stringify({
        version: 1,
        site_url: "https://agentrelay.com",
        org_id: "org",
        workspace_id: "workspace",
        job_id: "job",
        sharing_mode: "selected",
      }),
    );
    await expect(exportLocalWorkspace(options)).rejects.toThrow("all/new");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("selected-session hydration preserves locators and allows larger bounded context", async () => {
  const { directory, options } = await fixture();
  try {
    const first = await exportLocalWorkspace(options);
    const hydrated = await exportLocalWorkspace({
      ...options,
      sessionKeys: [first.exported.sessions[0]!.key],
      perSessionLimit: 20,
    });
    expect(hydrated.exported.events).toHaveLength(11);
    expect(hydrated.inventory.omittedConversations).toBe(0);
    expect(
      first.exported.events.every((e) =>
        hydrated.exported.events.some(
          (other) => other.id === e.id && other.eventId === e.eventId,
        ),
      ),
    ).toBe(true);
    await expect(
      exportLocalWorkspace({ ...options, sessionKeys: ["outside-scope"] }),
    ).rejects.toThrow("outside the admitted");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("local export writes private files without overwriting earlier exports", async () => {
  const { directory, options } = await fixture();
  try {
    const output = join(directory, "output", "export.json");
    await writeLocalExport(options, output);
    expect((await stat(output)).mode & 0o777).toBe(0o600);
    expect((await stat(`${output}.inventory.json`)).mode & 0o777).toBe(0o600);
    expect(JSON.parse(await readFile(output, "utf8")).events).toHaveLength(5);
    expect(
      JSON.parse(await readFile(`${output}.inventory.json`, "utf8"))
        .selectedEvents,
    ).toBe(5);
    const before = await readFile(output);
    await expect(writeLocalExport(options, output)).rejects.toThrow();
    expect(await readFile(output)).toEqual(before);
    expect((await readdir(join(directory, "output"))).sort()).toEqual([
      "export.json",
      "export.json.inventory.json",
    ]);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("an existing inventory prevents publication of a consumable export", async () => {
  const { directory, options } = await fixture();
  try {
    const output = join(directory, "export.json");
    const inventoryPath = `${output}.inventory.json`;
    await writeFile(inventoryPath, "existing accounting", { mode: 0o600 });
    await expect(writeLocalExport(options, output)).rejects.toThrow();
    expect(await readFile(inventoryPath, "utf8")).toBe("existing accounting");
    await expect(stat(output)).rejects.toThrow();
    expect(
      (await readdir(directory)).some((name) =>
        name.startsWith(".coordination-export-"),
      ),
    ).toBe(false);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("failed export publication removes only its newly published inventory", async () => {
  const { directory, options } = await fixture();
  try {
    const output = join(directory, "export.json");
    await writeFile(output, "existing export", { mode: 0o600 });
    const unrelated = join(directory, "unrelated.inventory.json");
    await writeFile(unrelated, "unrelated accounting", { mode: 0o600 });
    await expect(writeLocalExport(options, output)).rejects.toThrow();
    expect(await readFile(output, "utf8")).toBe("existing export");
    expect(await readFile(unrelated, "utf8")).toBe("unrelated accounting");
    await expect(stat(`${output}.inventory.json`)).rejects.toThrow();
    expect(
      (await readdir(directory)).some((name) =>
        name.startsWith(".coordination-export-"),
      ),
    ).toBe(false);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
