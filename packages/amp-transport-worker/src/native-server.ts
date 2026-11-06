// The reviewed services image already contains this route and the pinned CLI.
// Override its entrypoint, without starting ingest or giving it database keys.
// This program is static: provider keys and message text never enter argv.
export const NATIVE_SERVER_PROGRAM = String.raw`
import { createAmpTransportRoutes } from "./src/amp/routes.ts";
import { AMP_CLI_VERSION } from "./src/amp/provider.ts";
const app = createAmpTransportRoutes({ token: () => process.env.AMP_TRANSPORT_TOKEN });
app.get("/healthz", c => c.json({ ok: true, cliVersion: AMP_CLI_VERSION }));
const server = Bun.serve({
  hostname: "0.0.0.0",
  port: Number(process.env.PORT ?? 8080),
  maxRequestBodySize: 100 * 1024,
  idleTimeout: 65,
  fetch: app.fetch,
  error: () => new Response("Service unavailable", { status: 503 }),
});
`;
