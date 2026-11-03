import { afterEach, expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import {
  directoryCatalogFromProjection,
  writeDirectoryCatalog,
} from "../session-directory-catalog.ts";
const root = join(import.meta.dir, "../../.."),
  read = (p: string) => readFileSync(join(root, p), "utf8"),
  opened: PGlite[] = [];
afterEach(async () => {
  for (const db of opened.splice(0)) await db.close();
});
async function database() {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  opened.push(db);
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  await db.exec(read("supabase/bootstrap/session_directory_search_worker.sql"));
  for (const file of readdirSync(join(root, "supabase/migrations")).sort())
    await db.exec(read("supabase/migrations/" + file));
  await db.query(
    "INSERT INTO sessions.session_directory_search_token_policy(issuer,audience) VALUES('https://issuer.test','session-directory-search')",
  );
  return db;
}
async function seed(
  db: PGlite,
  session = "session",
  org = "org-a",
  workspace = "ws-a",
) {
  await db.query(
    `INSERT INTO sessions.session_directory(org_id,workspace_id,harness,session_id,created_at,last_activity_at,event_projection_version,event_projection_ready,event_projection_epoch,event_projected_at,catalog_read_version,catalog_read_epoch,catalog_read_projected_at,catalog_projected_at,catalog_models_complete,catalog_relationships_complete,catalog_models,event_models,event_projection_sources)
 VALUES($1,$2,'codex',$3,now(),now(),1,true,1,now(),1,1,now(),now(),true,true,ARRAY['catalog-only','same'],ARRAY['codex-auto-review','same'],'{"fixture":1}'::jsonb)`,
    [org, workspace, session],
  );
}
async function catalog(
  db: PGlite,
  session = "session",
  workspace = "ws-a",
  claimsWorkspace = workspace,
  org = "org-a",
) {
  const iat = Math.floor(Date.now() / 1000);
  await db.query("SELECT set_config('request.jwt.claims',$1,false)", [
    JSON.stringify({
      role: "session_directory_search_worker",
      iss: "https://issuer.test",
      aud: "session-directory-search",
      iat,
      exp: iat + 60,
      app_metadata: { org_id: org, workspace_ids: [claimsWorkspace] },
    }),
  ]);
  await db.exec("SET ROLE session_directory_search_worker");
  try {
    return (
      await db.query<{ body: any }>(
        "SELECT sessions.session_directory_desktop_catalog($1,'codex',$2) body",
        [workspace, session],
      )
    ).rows[0]!.body;
  } finally {
    await db.exec("RESET ROLE");
  }
}
test("readmodel returns complete dedup model union and real relationship review metadata", async () => {
  const db = await database();
  await seed(db);
  await seed(db, "parent");
  await seed(db, "child");
  await seed(db, "session", "org-b", "ws-b");
  await db.query(
    `INSERT INTO sessions.session_directory_relationships(org_id,workspace_id,harness,parent_session_id,relationship_uid,child_session_id,child_model,child_agent_name,child_agent_type) VALUES('org-a','ws-a','codex','parent','incoming','session','codex-auto-review','guardian','guardian'),('org-a','ws-a','codex','session','outgoing','child','model-child','helper','worker')`,
  );
  const body = await catalog(db);
  expect(body).toMatchObject({
    version: 1,
    exists: true,
    identity: {
      orgId: "org-a",
      workspaceId: "ws-a",
      source: "codex",
      sessionId: "session",
    },
    modelsComplete: true,
    catalog: { models: ["catalog-only", "codex-auto-review", "same"] },
    parents: [
      {
        relationshipUid: "incoming",
        parentSessionId: "parent",
        childSessionId: "session",
        childModel: "codex-auto-review",
        childAgentName: "guardian",
        childAgentType: "guardian",
      },
    ],
    children: [
      {
        relationshipUid: "outgoing",
        childSessionId: "child",
        childAgentType: "worker",
      },
    ],
    truncated: false,
  });
  expect(body.fingerprint).toMatch(/^[0-9a-f]{64}$/);
  expect(body).not.toHaveProperty("firstPrompt");
  await expect(catalog(db, "session", "ws-a", "ws-b")).rejects.toMatchObject({
    code: "42501",
  });
});
test("empty complete model union is explicit and incomplete old/overflow projection is refused", async () => {
  const db = await database();
  await seed(db);
  await db.exec(
    "UPDATE sessions.session_directory SET catalog_models='{}',event_models='{}'",
  );
  expect((await catalog(db)).catalog.models).toEqual([]);
  await db.exec(
    "UPDATE sessions.session_directory SET catalog_models_complete=false",
  );
  await expect(catalog(db)).rejects.toMatchObject({ code: "0A000" });
  await db.exec(
    "UPDATE sessions.session_directory SET catalog_models_complete=true,catalog_read_version=0",
  );
  await expect(catalog(db)).rejects.toMatchObject({ code: "0A000" });
});
test("unprojected parent edge index and changed epoch/projectedAt fail closed even for scoped child", async () => {
  const db = await database();
  await seed(db);
  await seed(db, "parent");
  await db.exec(
    "UPDATE sessions.session_directory SET catalog_relationships_complete=false WHERE session_id='parent'",
  );
  await expect(catalog(db)).rejects.toMatchObject({ code: "0A000" });
  await db.exec(
    "UPDATE sessions.session_directory SET catalog_relationships_complete=true,catalog_read_epoch=0 WHERE session_id='parent'",
  );
  await expect(catalog(db)).rejects.toMatchObject({ code: "0A000" });
  await db.exec(
    "UPDATE sessions.session_directory SET catalog_read_epoch=event_projection_epoch,catalog_read_projected_at=NULL WHERE session_id='parent'",
  );
  await expect(catalog(db)).rejects.toMatchObject({ code: "0A000" });
});
test("writer atomically publishes real review metadata and marks complete source epoch", async () => {
  const db = await database();
  await seed(db);
  await seed(db, "child");
  const projected = directoryCatalogFromProjection([
    { table: "session_catalog", row: { models: ["gpt-model"] } },
    {
      table: "session_relationships",
      row: {
        relationship_uid: "edge",
        child_session_id: "child",
        child_model: "codex-auto-review",
        child_agent_name: "guardian",
        child_agent_type: "guardian",
      },
    },
  ]);
  expect(projected.metadataVersion).toBe(1);
  expect(projected.modelsComplete).toBe(true);
  expect(projected.relationshipsComplete).toBe(true);
  await writeDirectoryCatalog(
    db as never,
    {
      orgId: "org-a",
      workspaceId: "ws-a",
      harness: "codex",
      sessionId: "session",
    },
    projected,
  );
  const value = await catalog(db);
  expect(value.children[0]).toMatchObject({
    relationshipUid: "edge",
    childModel: "codex-auto-review",
    childAgentName: "guardian",
    childAgentType: "guardian",
  });
  expect(value.catalog.models).toEqual([
    "codex-auto-review",
    "gpt-model",
    "same",
  ]);
});
test("projector never calls truncated model or relationship metadata complete", () => {
  const models = directoryCatalogFromProjection([
    {
      table: "session_catalog",
      row: { models: Array.from({ length: 65 }, (_, i) => "model" + i) },
    },
  ]);
  expect(models.models).toHaveLength(64);
  expect(models.modelsComplete).toBe(false);
  const edges = directoryCatalogFromProjection(
    Array.from({ length: 501 }, (_, i) => ({
      table: "session_relationships",
      row: {
        relationship_uid: "r" + i,
        child_session_id: "child" + i,
        child_model: null,
        child_agent_name: null,
        child_agent_type: null,
      },
    })),
  );
  expect(edges.relationships).toHaveLength(500);
  expect(edges.relationshipsComplete).toBe(false);
});
test("worker has no direct table grant; missing/tombstoned sessions are not live metadata", async () => {
  const db = await database();
  await seed(db);
  expect((await catalog(db, "missing")).exists).toBe(false);
  await db.exec("UPDATE sessions.session_directory SET deleted_at=now()");
  expect((await catalog(db)).exists).toBe(false);
  await db.exec("SET ROLE session_directory_search_worker");
  try {
    await expect(
      db.query("SELECT * FROM sessions.session_directory_relationships"),
    ).rejects.toMatchObject({ code: "42501" });
  } finally {
    await db.exec("RESET ROLE");
  }
});
