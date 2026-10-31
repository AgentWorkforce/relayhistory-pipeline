/**
 * The product read path in miniature: real requireAuth, scope only from verified claims,
 * the store chokepoint, and hydration that drops any hit without a live pointer. A failed
 * or interrupted store response is refused whole, once, with no retry and no rows. The
 * live set stands in for Supabase; #72 and #73 prove RLS there.
 */
import { Hono } from "hono";
import { requireAuth, type AppEnv } from "../../../src/shared/vendor.ts";
import type { SessionSearchStore } from "./store.ts";

export const liveKey = (org: string, ws: string, harness: string, session: string) => `${org}/${ws}/${harness}/${session}`;

export function searchApp(store: SessionSearchStore, live: Set<string>) {
  const app = new Hono<AppEnv>();
  app.use("*", async (c, next) => requireAuth(c as never, next));
  app.get("/v1/search", async (c) => {
    const auth = c.get("auth") as { orgId?: string; workspaceId?: string };
    const q = c.req.query("q");
    if (!q) return c.json({ error: { code: "bad_request" } }, 400);
    if (!auth.orgId || !auth.workspaceId) return c.json({ error: { code: "forbidden" } }, 403);
    try {
      const page = await store.search({ orgId: auth.orgId, workspaceIds: [auth.workspaceId] }, { query: q, limit: 20 });
      const hits = page.hits.filter((h) => live.has(liveKey(auth.orgId!, h.workspace_id, h.harness, h.session_id)));
      return c.json({ hits, nextCursor: page.nextCursor });
    } catch {
      return c.json({ error: { code: "search_unavailable" } }, 503);
    }
  });
  return app;
}
