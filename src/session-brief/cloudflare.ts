/**
 * Cloudflare Worker entry for the session brief (stacks/cloudflare/session-brief.ts,
 * runbooks/sessions-prod-session-brief-worker.md): a `scheduled` handler and nothing else.
 * workerd treats every named export of the entry module as an entrypoint (a constant or a
 * helper here fails the script at startup), so the logic lives in cloudflare-tick.ts.
 */
import { errorFields, log } from "../shared/log.ts";
import { runBriefInvocation, type BriefWorkerEnv } from "./cloudflare-tick.ts";

interface Ctx {
  waitUntil(work: Promise<unknown>): void;
}

export default {
  async scheduled(_controller: unknown, env: BriefWorkerEnv, ctx: Ctx): Promise<void> {
    const started = Date.now();
    try {
      const out = await runBriefInvocation(env, ctx);
      if (out.status !== "disabled")
        log.info("session brief invocation", { stage: "session-brief", status: out.status, duration_ms: Date.now() - started });
    } catch (err) {
      // A tick-level failure (database unreachable, a claim error): the next cron retries;
      // claimed wakeups come back when their lease lapses.
      log.error("session brief invocation failed", { stage: "session-brief", duration_ms: Date.now() - started, ...errorFields(err) });
      throw err;
    }
  },
};
