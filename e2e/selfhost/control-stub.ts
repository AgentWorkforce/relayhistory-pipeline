/**
 * Stands in for Control's token introspection endpoint
 * (POST /internal/v1/auth/introspect, contract v1.2, docs/local-stack.md), the
 * only external service the self-host pipeline needs at run time. The tokens
 * are the per-run synthetic ones run.ts generates. Nothing else about Control
 * is emulated, and the real introspector code in session-ingest and the read
 * API is what calls it.
 */
import { createServer } from "node:http";

interface TokenRecord { sub: string; org: string; workspace: string; scopes: string[] }
const secret = process.env.E2E_INTROSPECT_SECRET!;
const tokens = JSON.parse(process.env.E2E_TOKENS!) as Record<string, TokenRecord>;

createServer((req, res) => {
  const chunks: Buffer[] = [];
  req.on("data", (c: Buffer) => chunks.push(c));
  req.on("end", () => {
    const send = (status: number, body: unknown) => {
      res.writeHead(status, { "content-type": "application/json" });
      res.end(JSON.stringify(body));
    };
    if (req.url === "/healthz") return send(200, { ok: true });
    if (req.method !== "POST" || req.url !== "/internal/v1/auth/introspect") return send(404, { error: "not_found" });
    if (req.headers.authorization !== `Bearer ${secret}`) return send(401, { error: "unauthorized" });
    const input = JSON.parse(Buffer.concat(chunks).toString("utf8")) as { token: string; aud: string; workspace?: string };
    const record = tokens[input.token];
    if (!record) return send(200, { active: false, reason: "unknown", token_id: "none" });
    if (input.workspace && input.workspace !== record.workspace) return send(200, { active: false, reason: "workspace_mismatch", token_id: "t" });
    const now = Math.floor(Date.now() / 1000);
    send(200, {
      active: true,
      sub: record.sub,
      email: null,
      principal_type: "desktop-user",
      machine_id: null,
      org: record.org,
      workspace: record.workspace,
      scopes: record.scopes,
      aud: input.aud,
      iat: now - 10,
      exp: now + 3600,
      token_id: `tok-${record.sub}`,
    });
  });
}).listen(8090, "0.0.0.0");
