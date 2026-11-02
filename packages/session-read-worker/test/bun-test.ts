/**
 * `bun:test` for #135's suites under vitest: vitest's API, plus the one bun
 * matcher those suites use (`toStartWith`).
 */
import { expect } from "vitest";

export * from "vitest";

expect.extend({
  toStartWith(received: unknown, prefix: string) {
    const pass = typeof received === "string" && received.startsWith(prefix);
    return { pass, message: () => `expected ${this.utils.printReceived(received)} ${pass ? "not " : ""}to start with ${this.utils.printExpected(prefix)}` };
  },
});
