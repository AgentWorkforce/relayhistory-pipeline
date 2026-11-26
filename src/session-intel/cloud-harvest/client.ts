/**
 * Cloud API client for the harvest (docs/session-intel-cloud-harvest.md). Read-only: GETs to two routes under one
 * workspace, and read-only tool calls on the hosted shared-sessions MCP (the only Cloud surface that serves the
 * 96Bulls Reader's sessions, which Cloud's own list and events routes do not). The bearer comes only from
 * CLOUD_HARVEST_TOKEN (injected by `op run`) and is removed from the environment once read. Redirects are never
 * followed.
 */
export const TOKEN_ENV = "CLOUD_HARVEST_TOKEN";
const USER_AGENT = "session-intel-harvest/1";

export interface CloudClientOptions {
  base: string; workspaceId: string; maxRequests: number; fetchImpl?: typeof fetch; sleep?: (ms: number) => Promise<void>;
  /** Called synchronously before each request is sent, so a ledger can record it even if the process dies. */
  onRequest?: () => void;
}

export class CloudClient {
  readonly requests = { total: 0, retries: 0, byRoute: { list: 0, events: 0, mcp: 0 } };
  #token: string;
  #base: string;
  #mcp: string;
  #rpcId = 0;
  #fetch: typeof fetch;
  #sleep: (ms: number) => Promise<void>;
  constructor(readonly opts: CloudClientOptions, token: string) {
    if (!/^[A-Za-z0-9_-]{16,512}$/.test(token)) throw new Error(`${TOKEN_ENV} is not a bearer token`);
    const url = new URL(opts.base);
    if (url.protocol !== "https:" || url.username || url.password || url.search || url.hash) throw new Error("--cloud-api must be a plain https URL");
    if (!/^[0-9a-f-]{36}$/.test(opts.workspaceId)) throw new Error("--workspace must be a workspace uuid");
    if (!Number.isSafeInteger(opts.maxRequests) || opts.maxRequests <= 0) throw new Error("--max-requests must be a positive integer");
    this.#token = token;
    const api = `${url.origin}${url.pathname.replace(/\/+$/, "")}`;
    this.#base = `${api}/workspaces/${opts.workspaceId}`;
    this.#mcp = `${api}/mcp/shared-sessions`;
    this.#fetch = opts.fetchImpl ?? fetch;
    this.#sleep = opts.sleep ?? ((ms) => new Promise((r) => setTimeout(r, ms)));
  }

  static fromEnv(opts: CloudClientOptions): CloudClient {
    const token = process.env[TOKEN_ENV]?.trim() ?? "";
    delete process.env[TOKEN_ENV];
    return new CloudClient(opts, token);
  }

  /** Redacts the token (whole or either half) from any message before it can be printed. */
  redact(s: string): string {
    const t = this.#token, h = t.length >> 1;
    return [t, t.slice(0, h), t.slice(h)].reduce((acc, part) => part.length >= 8 ? acc.split(part).join("[token]") : acc, s);
  }

  /**
   * The shared-sessions list tool takes no workspace: it reads the credential's own. Before any read, the
   * credential's workspace (`describe_capabilities`) must be the one this harvest was authorized for.
   */
  async attestWorkspace(): Promise<void> {
    const caps = await this.callTool("describe_capabilities", {});
    const ws = (caps.workspace as { workspace_id?: unknown } | undefined)?.workspace_id;
    if (ws !== this.opts.workspaceId) throw new Error("the credential is not bound to the requested workspace");
  }

  get workspaceId(): string { return this.opts.workspaceId; }

  get(route: "list" | "events", path: string): Promise<unknown> {
    return this.#send(route, this.#base + path, { method: "GET" }).then((r) => r.json());
  }

  /**
   * Calls one shared-sessions MCP tool and returns its JSON payload. The tool answers with text: an optional
   * preamble and then one JSON object. A tool error (`isError`) is thrown as McpToolError with its first line,
   * which names the condition ("No shared session was found ..."), never transcript content.
   */
  async callTool(name: McpReadTool, args: Record<string, unknown>): Promise<Record<string, unknown>> {
    const body = JSON.stringify({ jsonrpc: "2.0", id: ++this.#rpcId, method: "tools/call", params: { name, arguments: args } });
    const res = await this.#send("mcp", this.#mcp, { method: "POST", body, headers: { "content-type": "application/json", accept: "application/json, text/event-stream" } });
    const raw = await res.text();
    // Streamable HTTP may answer as an event stream; the JSON-RPC response is its last `data:` line.
    const json = res.headers.get("content-type")?.includes("text/event-stream")
      ? raw.split("\n").filter((l) => l.startsWith("data:")).map((l) => l.slice(5).trim()).at(-1) ?? ""
      : raw;
    const msg = JSON.parse(json) as { result?: { isError?: boolean; content?: Array<{ type: string; text?: string }>; structuredContent?: Record<string, unknown> }; error?: { code: number; message: string } };
    if (msg.error) throw new McpToolError(`rpc_${msg.error.code}`);
    const text = msg.result?.content?.find((c) => c.type === "text")?.text ?? "";
    if (msg.result?.isError) throw new McpToolError(text.split("\n")[0]!.slice(0, 200));
    if (msg.result?.structuredContent) return msg.result.structuredContent;
    if (text.trimStart().startsWith("{")) return JSON.parse(text);
    const at = text.indexOf("\n{");
    if (at < 0) throw new McpToolError("tool answered without a JSON payload");
    return JSON.parse(text.slice(at + 1));
  }

  async #send(route: "list" | "events" | "mcp", url: string, init: { method: string; body?: string; headers?: Record<string, string> }): Promise<Response> {
    for (let attempt = 0; ; attempt++) {
      if (this.requests.total >= this.opts.maxRequests) throw new HarvestCap();
      this.requests.total++; this.requests.byRoute[route]++;
      this.opts.onRequest?.();
      let res: Response;
      try {
        res = await this.#fetch(url, { method: init.method, body: init.body, headers: { accept: "application/json", ...init.headers, authorization: `Bearer ${this.#token}`, "user-agent": USER_AGENT }, redirect: "manual" });
      } catch (e) {
        if (attempt >= 5) throw new Error(this.redact(`transport: ${(e as Error).message}`));
        this.requests.retries++; await this.#sleep(1000 * 2 ** attempt); continue;
      }
      if (res.status === 200) return res;
      if ([429, 500, 502, 503, 504].includes(res.status) && attempt < 5) {
        const ra = Number(res.headers.get("retry-after"));
        this.requests.retries++; await this.#sleep(Number.isFinite(ra) && ra > 0 ? Math.min(ra, 120) * 1000 : 1000 * 2 ** attempt); continue;
      }
      throw new HttpError(res.status, route);
    }
  }
}

/** The only MCP tools the harvest calls: both are reads. */
export type McpReadTool = "list_session_summaries" | "get_shared_session" | "describe_capabilities";
export class McpToolError extends Error {}
export class HarvestCap extends Error { constructor() { super("request cap reached"); } }
export class HttpError extends Error { constructor(readonly status: number, route: string) { super(`${route}: HTTP ${status}`); } }
