import { createHash, randomBytes } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { scrubJson, scrubText } from "../src/lib/scrub.js";
import { SCRUB_RULESET_VERSION, scrubRecords } from "../src/lib/pipeline/raw-chunks.js";
import { scrubText as v6 } from "./fixtures/scrub-ruleset-6.js";

const encoder = new TextEncoder(), decoder = new TextDecoder();
const records = (value: unknown) => JSON.parse(decoder.decode(scrubRecords(encoder.encode(JSON.stringify(value))).scrubbed));
const replay = 'idempotency_key="public-replay-20261109-1"';
const tool = `to="worker@machine" text="Hello Worker!" ${replay}`;

describe("scrub ruleset 7 public replay selector", () => {
  it("freezes shipped version 6 and advances the stored ruleset version", () => {
    const source = readFileSync(fileURLToPath(new URL("./fixtures/scrub-ruleset-6.ts", import.meta.url).href), "utf8").replace(/\r\n/g, "\n");
    expect(createHash("sha256").update(source).digest("hex")).toBe("397f820a70a8ae83cfdfae9f4153de59b26fd41fb35ad7be25b38cb52ad2d526");
    const raw = readFileSync(fileURLToPath(new URL("../src/lib/pipeline/raw-chunks.ts", import.meta.url).href), "utf8").replace(/\r\n/g, "\n");
    const segmenter = /\/\/ <scrub-segmenter>[\s\S]*?\/\/ <\/scrub-segmenter>/.exec(raw)![0];
    expect(createHash("sha256").update(`${source}\n${segmenter}`).digest("hex")).toBe("28edb23b9ff94299d530213a04026210eb848399475b740e026ecc85980b3a11");
    expect(SCRUB_RULESET_VERSION).toBe(8);
    expect(v6(tool)).not.toBe(tool);
  });

  it("keeps benign tool input through scalar and escaped record scrubbing", () => {
    expect(scrubText(tool)).toBe(tool);
    let text = tool;
    for (let depth = 0; depth < 5; depth++) {
      expect(scrubText(text)).toBe(text);
      expect(records({ payload: { kind: "tool_use", text: scrubText(text) } }).payload.text).toBe(text);
      text = JSON.stringify(text);
    }
    expect(scrubJson({ arguments: { idempotency_key: "public-replay-20261109-1", text: "Hello Worker!" } })).toEqual({ arguments: { idempotency_key: "public-replay-20261109-1", text: "Hello Worker!" } });
  });

  it("exempts only the exact identifier, case-insensitively", () => {
    for (const key of ["idempotency_key", "IDEMPOTENCY_KEY", "Idempotency_Key"]) {
      for (const separator of ["=", ":", " : ", "\t=\t"]) {
        const text = `${key}${separator}public-replay-id`;
        expect(scrubText(text)).toBe(text);
        expect(records({ text }).text).toBe(text);
      }
    }
    for (const key of ["api_key", "token", "password", "my_idempotency_key", "idempotency_token", "idempotency_api_key"]) {
      const text = `${key}=private-assignment-value`;
      expect(scrubText(text)).not.toContain("private-assignment-value");
      expect(JSON.stringify(records({ text }))).not.toContain("private-assignment-value");
    }
    const mixed = `${replay} API_KEY=private-assignment-value`;
    expect(scrubText(mixed)).toContain(replay);
    expect(scrubText(mixed)).not.toContain("private-assignment-value");
  });

  it("still removes independently recognizable credentials under the public field", () => {
    const token = randomBytes(24).toString("hex");
    const header = Buffer.from(JSON.stringify({ alg: "HS256", typ: "JWT" })).toString("base64url");
    const payload = Buffer.from(JSON.stringify({ sub: "synthetic-test-caller" })).toString("base64url");
    const credentials = [`sk-${token}`, `Bearer ${token}`, `${header}.${payload}.${randomBytes(24).toString("base64url")}`];
    for (const credential of credentials) {
      const text = `idempotency_key="${credential}"`;
      expect(scrubText(text)).not.toContain(credential);
      expect(JSON.stringify(scrubJson({ idempotency_key: credential }))).not.toContain(credential);
      expect(JSON.stringify(records({ text }))).not.toContain(credential);
    }
    for (const value of [tool, { nested: tool }, [tool]]) {
      expect(JSON.stringify(scrubJson({ password: value }))).not.toContain("public-replay-20261109-1");
      expect(JSON.stringify(records({ password: value }))).not.toContain("public-replay-20261109-1");
    }
  });
});
