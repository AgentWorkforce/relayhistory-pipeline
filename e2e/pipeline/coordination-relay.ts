/** Loopback recording transport for the real HTTP sender; no Relay routing is claimed. */
import { createServer } from "node:http";

const token = process.env.E2E_COORDINATION_RELAY_TOKEN;
if (!token) throw new Error("recording transport needs its per-run token");
const messages: unknown[] = [];
let rejectSends = false;
createServer(async (req, res) => {
  res.setHeader("content-type", "application/json");
  if (req.headers.authorization !== `Bearer ${token}`) {
    res.writeHead(401).end("{}");
    req.resume();
    return;
  }
  if (req.method === "GET" && req.url === "/messages") {
    res.end(JSON.stringify({ messages }));
    return;
  }
  let raw = "";
  try {
    for await (const chunk of req) {
      raw += chunk.toString();
      if (raw.length > 8_192) { res.writeHead(413).end("{}"); return; }
    }
  } catch { res.writeHead(400).end("{}"); return; }
  let body: any;
  try { body = JSON.parse(raw); } catch { res.writeHead(400).end("{}"); return; }
  if (req.method === "POST" && req.url === "/mode" && typeof body?.rejectSends === "boolean") {
    rejectSends = body.rejectSends;
    res.end("{}");
    return;
  }
  if (req.method !== "POST" || req.url !== "/relay") { res.writeHead(404).end("{}"); return; }
  if (rejectSends) { res.writeHead(503).end("{}"); return; }
  if (body?.from !== "relayhistory-coordination" || body.orgId !== "org_a" || body.workspaceId !== "ws_a"
    || body.to?.harness !== "claude" || typeof body.to.sessionId !== "string"
    || typeof body.text !== "string" || body.text.length > 600 || typeof body.advisoryId !== "string") {
    res.writeHead(400).end("{}");
    return;
  }
  messages.push(body);
  res.writeHead(202).end("{}");
}).listen(3123, "0.0.0.0");
