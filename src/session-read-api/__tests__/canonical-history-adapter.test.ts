import { expect, test } from "bun:test";
import {
  CONTENT_TRUNCATION_NOTE,
} from "../page.ts";
import {
  canonicalHistoryAdapterConfig,
  readCanonicalHistoryPage,
  type CanonicalHistoryAdapterConfig,
} from "../canonical-history-adapter.ts";

const identity = {
  orgId: "org-a",
  workspaceId: "ws-a",
  source: "claude",
  sessionId: "session-a",
};
const config: CanonicalHistoryAdapterConfig = {
  enabled: true,
  endpointOrigin: "https://canonical.invalid",
  orgId: identity.orgId,
  workspaceId: identity.workspaceId,
  scopes: [{ source: identity.source, sessionId: identity.sessionId }],
  deadlineMs: 1000,
  maxResponseBytes: 4096,
  maxRowsPerPage: 20,
};
const input = {
  ...identity,
  bearer: "synthetic.jwt.token",
  limit: 2,
  maxContent: 5,
};
const row = {
  org_id: identity.orgId,
  workspace_id: identity.workspaceId,
  user_id: "user-a",
  source: identity.source,
  session_id: identity.sessionId,
  event_id: "event-a",
  kind: "message",
  type: "assistant",
  ts: "2026-11-01T00:00:00.123456Z",
  actor_name: null,
  actor_role: "assistant",
  subagent_id: null,
  machine_id: "machine-a",
  project_id: null,
  task_ref: { id: "task-a" },
  task_title: null,
  content: "abcdef",
  tags: ["one"],
  tool_name: null,
  tool_status: null,
  files_touched: [],
  record: { payload: { message_id: "native-a" } },
};
const response = (overrides: Record<string, unknown> = {}) =>
  new Response(
    JSON.stringify({
      ...identity,
      generation: "a".repeat(64),
      exists: true,
      rows: [{ table: "convergence_events", row }],
      nextCursor: null,
      correlationId: "correlation-a",
      ...overrides,
    }),
    {
      headers: { "x-relayhistory-workspace-id": identity.workspaceId },
    },
  );

test("config is default-off and requires an exact HTTPS tenant/session coverage set", () => {
  expect(canonicalHistoryAdapterConfig({})).toEqual({ enabled: false });
  const parsed = canonicalHistoryAdapterConfig({
    HISTORY_CANONICAL_CANARY_ENABLED: "true",
    HISTORY_CANONICAL_CANARY_ORIGIN: "https://canonical.invalid",
    HISTORY_CANONICAL_CANARY_ORG_ID: identity.orgId,
    HISTORY_CANONICAL_CANARY_WORKSPACE_ID: identity.workspaceId,
    HISTORY_CANONICAL_CANARY_SCOPES_JSON: JSON.stringify([
      { source: identity.source, sessionId: identity.sessionId },
    ]),
  });
  expect(parsed).toMatchObject({
    enabled: true,
    endpointOrigin: "https://canonical.invalid",
    orgId: identity.orgId,
    workspaceId: identity.workspaceId,
    deadlineMs: 7500,
    maxRowsPerPage: 200,
  });
  expect(() =>
    canonicalHistoryAdapterConfig({
      HISTORY_CANONICAL_CANARY_ENABLED: "true",
      HISTORY_CANONICAL_CANARY_ORIGIN: "http://canonical.invalid",
      HISTORY_CANONICAL_CANARY_ORG_ID: identity.orgId,
      HISTORY_CANONICAL_CANARY_WORKSPACE_ID: identity.workspaceId,
      HISTORY_CANONICAL_CANARY_SCOPES_JSON: "[]",
    }),
  ).toThrow("Canonical History adapter refused");
});

test("calls only the canonical replay endpoint, verifies attestation and maps tenant-stripped rows", async () => {
  let called = 0;
  const page = await readCanonicalHistoryPage(config, input, async (request, init) => {
    called++;
    const url = new URL(String(request));
    expect(url.pathname).toBe("/v1/canonical/sessions/session-a");
    expect(url.pathname).not.toContain("/v1/events");
    expect(Object.fromEntries(url.searchParams)).toMatchObject({
      workspace: identity.workspaceId,
      source: identity.source,
      table: "convergence_events",
      view: "replay",
      limit: "2",
      maxContent: "5",
    });
    expect(new Headers(init?.headers).get("authorization")).toBe(
      `Bearer ${input.bearer}`,
    );
    return response({
      rows: [
        {
          table: "convergence_events",
          row: { ...row, org_id: undefined, workspace_id: undefined },
        },
      ],
    });
  });
  expect(called).toBe(1);
  expect(page).toMatchObject({
    ...identity,
    generation: "a".repeat(64),
    exists: true,
    nextCursor: null,
  });
  expect(page.events[0]).toMatchObject({
    eventId: "event-a",
    content: `abcde${CONTENT_TRUNCATION_NOTE}`,
    contentTruncated: true,
    taskRef: { id: "task-a" },
  });
});

test("refuses outside configured coverage before token transport", async () => {
  let calls = 0;
  await expect(
    readCanonicalHistoryPage(
      config,
      { ...input, sessionId: "other" },
      async () => {
        calls++;
        return response();
      },
    ),
  ).rejects.toMatchObject({ reason: "canonical_history_outside_coverage" });
  expect(calls).toBe(0);
});

test("refuses foreign rows and generation drift without returning a partial page", async () => {
  await expect(
    readCanonicalHistoryPage(config, input, async () =>
      response({
        rows: [
          {
            table: "convergence_events",
            row: { ...row, workspace_id: "foreign" },
          },
        ],
      }),
    ),
  ).rejects.toMatchObject({ reason: "canonical_history_tenant_mismatch" });
  await expect(
    readCanonicalHistoryPage(
      config,
      { ...input, generation: "b".repeat(64) },
      async () => response(),
    ),
  ).rejects.toMatchObject({ reason: "canonical_history_response" });
});

test("honestly refuses oversized bodies and missing workspace attestation", async () => {
  const tight = { ...config, maxResponseBytes: 1024 };
  await expect(
    readCanonicalHistoryPage(tight, input, async () =>
      new Response("x".repeat(1025), {
        headers: { "x-relayhistory-workspace-id": identity.workspaceId },
      }),
    ),
  ).rejects.toMatchObject({ reason: "canonical_history_response_oversize" });
  await expect(
    readCanonicalHistoryPage(config, input, async () =>
      new Response(JSON.stringify({}), {
        headers: { "x-relayhistory-workspace-id": "foreign" },
      }),
    ),
  ).rejects.toMatchObject({ reason: "canonical_history_attestation" });
});

test("deadline abort is typed retryable and does not trigger another backend", async () => {
  let calls = 0;
  await expect(
    readCanonicalHistoryPage(
      { ...config, deadlineMs: 10 },
      input,
      async (_request, init) => {
        calls++;
        return await new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener(
            "abort",
            () => reject(new Error("aborted")),
            { once: true },
          );
        });
      },
    ),
  ).rejects.toMatchObject({
    reason: "canonical_history_deadline",
    retryable: true,
  });
  expect(calls).toBe(1);

  await expect(
    readCanonicalHistoryPage(
      { ...config, deadlineMs: 10 },
      input,
      async () =>
        new Response(
          new ReadableStream<Uint8Array>({
            pull: () => new Promise<void>(() => {}),
          }),
          {
            headers: {
              "x-relayhistory-workspace-id": identity.workspaceId,
            },
          },
        ),
    ),
  ).rejects.toMatchObject({
    reason: "canonical_history_deadline",
    retryable: true,
  });
});
