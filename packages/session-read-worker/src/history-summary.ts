/** Public framing only. The keyring is an explicitly injected READ port; the
 * default worker has no key loader or fallback and refuses until one is wired. */
import { rawStoreFromEnv } from "../../pipeline/src/lib/pipeline/adapter.ts";
import { objectKey } from "../../object-store/src/index.ts";
import { CanonicalReadError } from "../../../src/session-read-api/canonical.ts";
import {
  readClaimedHistorySummary,
  type HistorySummaryReadDeps,
  type HistorySummaryReadKeyring,
} from "../../../src/shared/history-summary-reader.ts";
import {
  canonicalTokenTenant,
  canonicalHistorySummaryClaimReader,
  type CanonicalEnv,
} from "./canonical.ts";
export interface HistorySummaryEnv extends CanonicalEnv {
  CANONICAL_HISTORY_READ_ENABLED?: string;
}
export async function canonicalHistorySummaryHandle(
  request: Request,
  env: HistorySummaryEnv,
  fetcher: typeof fetch = fetch,
  keyring?: HistorySummaryReadKeyring,
  override?: HistorySummaryReadDeps,
): Promise<Response> {
  const controller = new AbortController(),
    correlationId = crypto.randomUUID();
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    if (
      env.CANONICAL_READ_ENABLED !== "true" ||
      env.CANONICAL_HISTORY_READ_ENABLED !== "true" ||
      env.CANONICAL_READ_MODE !== "pages"
    )
      throw new CanonicalReadError("not_configured");
    const url = new URL(request.url),
      match = /^\/v1\/canonical\/summaries\/([^/]+)$/.exec(url.pathname);
    if (request.method !== "GET" || !match)
      throw new CanonicalReadError("route", 400);
    for (const key of url.searchParams.keys())
      if (
        !["workspace", "source", "kind"].includes(key) ||
        url.searchParams.getAll(key).length !== 1
      )
        throw new CanonicalReadError("parameter", 400);
    const workspace = url.searchParams.get("workspace"),
      source = url.searchParams.get("source"),
      kind = url.searchParams.get("kind");
    let sessionId: string;
    try {
      sessionId = decodeURIComponent(match[1]!);
    } catch {
      throw new CanonicalReadError("parameter", 400);
    }
    if (
      !workspace ||
      !source ||
      !/^[a-z0-9][a-z0-9._-]{0,63}$/.test(source) ||
      !sessionId ||
      [...sessionId].length > 255 ||
      (kind !== "short" && kind !== "long")
    )
      throw new CanonicalReadError("parameter", 400);
    const bearer = /^Bearer\s+(\S+)$/iu.exec(
      request.headers.get("authorization") ?? "",
    )?.[1];
    if (!bearer) throw new CanonicalReadError("authorization", 401);
    const identity = {
      ...canonicalTokenTenant(bearer, workspace),
      source,
      sessionId,
    };
    const signal = AbortSignal.any([request.signal, controller.signal]);
    signal.throwIfAborted();
    const boundedFetch: typeof fetch = (input, init) =>
      fetcher(input, {
        ...init,
        signal: init?.signal ? AbortSignal.any([init.signal, signal]) : signal,
      });
    let deps = override;
    if (!deps) {
      if (!keyring)
        throw new CanonicalReadError("history_keyring_unconfigured");
      const raw = rawStoreFromEnv(env, boundedFetch);
      if (!raw || raw.provider !== "s3")
        throw new CanonicalReadError("not_configured");
      const claim = canonicalHistorySummaryClaimReader(
        env,
        bearer,
        signal,
        kind,
        boundedFetch,
      );
      deps = {
        storage: {
          provider: "s3",
          bucket: raw.bucket,
          region: env.PIPELINE_RAW_S3_REGION ?? "us-east-1",
        },
        keyring,
        claim: () => claim(identity),
        read: (key) => raw.store.getObject(objectKey(key)),
      };
    }
    const result = await Promise.race([
      readClaimedHistorySummary(deps, { identity, kind, signal }),
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => {
          reject(new CanonicalReadError("deadline"));
          controller.abort();
        }, 8000);
      }),
    ]);
    signal.throwIfAborted();
    if (result.status === "unavailable")
      throw new CanonicalReadError("history_summary_unavailable");
    return Response.json(
      {
        version: 1,
        identity,
        kind,
        status: result.status,
        summary: result.status === "ready" ? result.summary : null,
        model: result.status === "ready" ? result.model : null,
        builtAt: result.status === "ready" ? result.builtAt : null,
        source: result.source,
        state: result.state,
        correlationId,
      },
      {
        headers: {
          "X-Relayhistory-Workspace-Id": workspace,
          "Cache-Control": "private, no-store",
        },
      },
    );
  } catch (error) {
    const e =
      error instanceof CanonicalReadError
        ? error
        : new CanonicalReadError("history_summary_unavailable");
    return Response.json(
      {
        error: {
          code:
            e.status === 400
              ? "bad_request"
              : e.status === 401
                ? "unauthorized"
                : "session_read_unavailable",
          message: e.message,
        },
        correlationId,
      },
      {
        status: e.status,
        headers: {
          "Cache-Control": "private, no-store",
          "X-Session-Read-Reason": e.reason,
        },
      },
    );
  } finally {
    if (timer) clearTimeout(timer);
    controller.abort();
  }
}
