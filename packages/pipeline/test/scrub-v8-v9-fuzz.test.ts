import { describe, expect, it } from "vitest";
import { scrubText, scrubJson } from "../src/lib/scrub.js";
import { scrubText as v7, scrubJson as v7Json } from "./fixtures/scrub-ruleset-7.js";
import { scrubText as v8, scrubJson as v8Json } from "./fixtures/scrub-ruleset-8.js";

// Deterministic synthetic markers, not provider-shaped credentials. The
// targeted causal matrix covers every requested entry point separately.
const INPUTS = Number(process.env.SCRUB_FUZZ_INPUTS ?? 4096);
const SEED = Number(process.env.SCRUB_FUZZ_SEED ?? 0xc001beef);

describe("v7/v8 to v9 assignment differential", () => {
  it("never re-emits a marker either predecessor removed and is idempotent", () => {
    let seed = SEED >>> 0;
    const shapes = new Set<number>(), endings = new Set<boolean>();
    const pick = <T,>(xs: T[]): T => {
      seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
      // Use the full state, not low-bit modulo (an LCG's low bits can make
      // even-sized choices repeat and silently omit terminated cases).
      return xs[Math.floor((seed / 0x100000000) * xs.length)]!;
    };
    for (let i = 0; i < INPUTS; i++) {
      const a = `zqpriv${i.toString(36)}alphazq`, b = `zqpriv${i.toString(36)}betazq`;
      const label = pick(["password", "token", "api_key", "client_secret", "credentials"]);
      const sep = pick(["=", ":", " : ", String.raw`\u003d`, String.raw`\u003D`, String.raw`\u003a`, String.raw`\u003A`, String.raw`\x3d`, "%3d", "%3D", "%3a", "%3A"]);
      const quote = pick(['"', "'", String.fromCharCode(96)]);
      const closed = pick([quote, ""]);
      const shape = pick([0, 1, 2, 3, 4, 5]);
      shapes.add(shape);
      endings.add(closed !== "");
      const privateMarkers = shape < 4 ? [a, b] : [];
      let text = [
        `idempotency_key=${label}${sep}${quote}${a} ${b}${closed}`,
        `idempotency_key=${quote}${label}${sep}${a} ${b}${closed}`,
        `idempotency_key=${label}${sep}${a} ${b}`,
        `"${label}": ${quote}${a} ${b}${closed}`,
        `idempotency_key=public-${a}-${b}`,
        `"password": "op://vault/${a}/${b}"`,
      ][shape]!;
      const benign = shape >= 4;
      const depth = pick([0, 1, 2, 3, 4]);
      for (let d = 0; d < depth; d++) text = JSON.stringify(text);
      const current = scrubText(text), older = [v7(text), v8(text)];
      expect(scrubText(current), `text idempotence ${i}`).toBe(current);
      for (const marker of [a, b])
        if (older.some((value) => !value.includes(marker)))
          expect(current, `re-emitted ${i}`).not.toContain(marker);
      for (const marker of privateMarkers) expect(current, `secret ${i}`).not.toContain(marker);
      if (benign) expect(current, `public compatibility ${i}`).toBe(older[1]);

      const record = { idempotency_key: text, content: text };
      const json = scrubJson(record), serialized = JSON.stringify(json);
      expect(scrubJson(json), `JSON idempotence ${i}`).toEqual(json);
      const previous = [v7Json(record), v8Json(record)].map((value) => JSON.stringify(value));
      for (const marker of [a, b])
        if (previous.some((value) => !value.includes(marker)))
          expect(serialized, `JSON re-emitted ${i}`).not.toContain(marker);
      for (const marker of privateMarkers) expect(serialized, `JSON secret ${i}`).not.toContain(marker);
    }
    expect(shapes.size).toBe(6);
    expect(endings.size).toBe(2);
  });
});
