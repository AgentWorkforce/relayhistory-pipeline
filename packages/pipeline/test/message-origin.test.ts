import { describe, expect, it } from "vitest";
import {
  MESSAGE_ORIGINS,
  isMessageOrigin,
  messageOrigin,
  messageSubagent,
  originKindLabel,
} from "../src/lib/pipeline/message-origin.js";

/** A relayhistory `session_event` payload, as the desktop uploads it. */
const user = (over: Record<string, unknown> = {}) => ({
  role: "user", kind: "text", text: "yes", is_meta: 0, is_sidechain: 0,
  control_kind: null, agent_id: null, subagent_session_id: null, ...over,
});

describe("messageOrigin", () => {
  it("labels a human-typed message from origin_kind = human", () => {
    expect(messageOrigin("session_event", user({ origin_kind: "human" }))).toBe("human");
  });

  it("labels peer and task-notification origins as agent traffic", () => {
    expect(messageOrigin("session_event", user({ origin_kind: "peer" }))).toBe("peer");
    expect(messageOrigin("session_event", user({ origin_kind: "task-notification" }))).toBe("task-notification");
    // relayhistory's own control classification wins even with no origin recorded.
    expect(messageOrigin("session_event", user({ control_kind: "task_notification" }))).toBe("task-notification");
    expect(messageOrigin("session_event", user({ control_kind: "task_notification", origin_kind: "human" }))).toBe("task-notification");
  });

  it("labels a sidechain user message as the parent agent prompting a subagent, whatever its origin", () => {
    expect(messageOrigin("session_event", user({ is_sidechain: 1 }))).toBe("subagent");
    expect(messageOrigin("session_event", user({ is_sidechain: "true", origin_kind: "human" }))).toBe("subagent");
  });

  it("never labels a control or meta row human", () => {
    expect(messageOrigin("session_event", user({ control_kind: "command_output", origin_kind: "human" }))).toBe("system");
    expect(messageOrigin("session_event", user({ is_meta: 1, origin_kind: "human" }))).toBe("system");
    expect(messageOrigin("session_event", user({ is_meta: "1" }))).toBe("system");
  });

  it("gives no label when the upload recorded no origin (legacy clients), so human-only search excludes it", () => {
    expect(messageOrigin("session_event", user())).toBeNull();
    expect(messageOrigin("session_event", user({ origin_kind: "" }))).toBeNull();
    expect(messageOrigin("session_event", user({ origin_kind: null }))).toBeNull();
  });

  it("maps an unknown origin kind to other, never human", () => {
    expect(messageOrigin("session_event", user({ origin_kind: "coordinator" }))).toBe("other");
    expect(originKindLabel("TASK_NOTIFICATION")).toBe("task-notification");
    expect(originKindLabel("Human")).toBe("human");
  });

  it("labels assistant output agent, history prompts human, and leaves tool activity unlabeled", () => {
    expect(messageOrigin("session_event", { role: "assistant", text: "done" })).toBe("agent");
    expect(messageOrigin("history", { prompt: "yes" })).toBe("human");
    expect(messageOrigin("tool_call", { name: "Bash" })).toBeNull();
    expect(messageOrigin("file_edit", { file_path: "a.ts" })).toBeNull();
    expect(messageOrigin("session_event", null)).toBeNull();
  });

  it("only ever returns a declared label", () => {
    for (const origin of ["human", "peer", "task-notification", "x", "user"])
      expect(MESSAGE_ORIGINS).toContain(messageOrigin("session_event", user({ origin_kind: origin })));
    expect(isMessageOrigin("human")).toBe(true);
    expect(isMessageOrigin("Human")).toBe(false);
    expect(isMessageOrigin(undefined)).toBe(false);
  });
});

describe("messageSubagent", () => {
  it("uses subagent_session_id, else a sidechain row's agent_id, never a root row's agent_id", () => {
    expect(messageSubagent("session_event", user({ subagent_session_id: "sub-1" }))).toBe("sub-1");
    expect(messageSubagent("session_event", user({ is_sidechain: 1, agent_id: "a-7" }))).toBe("a-7");
    expect(messageSubagent("session_event", user({ agent_id: "a-7" }))).toBeNull();
    expect(messageSubagent("tool_call", { subagent_session_id: "sub-1" })).toBeNull();
  });
});
