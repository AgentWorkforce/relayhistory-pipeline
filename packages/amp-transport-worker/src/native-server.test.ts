import { expect, test } from "bun:test";
import { NATIVE_SERVER_PROGRAM } from "./native-server.ts";

test("the actual container entrypoint boots without ingest database credentials", async () => {
  const child = Bun.spawn({
    cmd: [process.execPath, "-e", NATIVE_SERVER_PROGRAM + "\nconsole.log(server.port);"],
    cwd: new URL("../../../", import.meta.url).pathname,
    env: { PATH: process.env.PATH, PORT: "0", AMP_TRANSPORT_TOKEN: "fixture-only-" + "a".repeat(40) },
    stdout: "pipe", stderr: "pipe",
  });
  const reader = child.stdout.getReader();
  try {
    const line = await reader.read();
    const port = Number(new TextDecoder().decode(line.value).trim());
    expect(port).toBeGreaterThan(0);
    const base = `http://127.0.0.1:${port}`;
    const health = await fetch(base + "/healthz");
    expect(health.status).toBe(200);
    expect((await health.json()).ok).toBe(true);
    const unauthenticated = await fetch(base + "/v1/internal/amp/operation", {
      method: "POST", headers: { "Content-Type": "application/json" }, body: "{}",
    });
    expect(unauthenticated.status).toBe(401);
    expect((await fetch(base + "/v1/internal/pipeline/replay")).status).toBe(404);
  } finally {
    await reader.cancel();
    child.kill();
    await child.exited;
  }
}, 10_000);
