import { describe, expect, it } from "bun:test";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { validationRecordFrom, validationRecordOverride, waitForValidationRecord } from "../cert-validation.ts";

const HOST = "proof-sessions.agentrelay.com";
const ready = [{ DomainName: HOST, ResourceRecord: { Name: "_abc.proof-sessions.agentrelay.com.", Type: "CNAME", Value: "_def.acm-validations.aws." } }];
const fast = { attempts: 3, interval: Duration.millis(1) };

describe("ACM validation record", () => {
  it("is undefined, never an empty name, while ACM has not populated it (the Stage A race)", () => {
    expect(validationRecordFrom(undefined, HOST)).toBeUndefined();
    expect(validationRecordFrom([], HOST)).toBeUndefined();
    expect(validationRecordFrom([{ DomainName: HOST }], HOST)).toBeUndefined();
    expect(validationRecordFrom([{ DomainName: HOST, ResourceRecord: { Name: "", Value: "" } }], HOST)).toBeUndefined();
    expect(validationRecordFrom([{ DomainName: HOST, ResourceRecord: { Name: "  ", Value: "x" } }], HOST)).toBeUndefined();
    expect(validationRecordFrom(ready, HOST)).toEqual({ name: "_abc.proof-sessions.agentrelay.com", value: "_def.acm-validations.aws" });
  });

  it("waits until ACM populates the record", async () => {
    let n = 0;
    const r = await Effect.runPromise(waitForValidationRecord(() => (++n < 3 ? [{ DomainName: HOST }] : ready), HOST, "arn:cert", fast));
    expect(r.name).toBe("_abc.proof-sessions.agentrelay.com");
    expect(n).toBe(3);
  });

  it("fails loudly after the bounded wait instead of emitting an empty record", async () => {
    let calls = 0;
    const exit = await Effect.runPromiseExit(waitForValidationRecord(() => (calls++, [{ DomainName: HOST }]), HOST, "arn:aws:acm:us-west-2:1:certificate/x", fast));
    expect(exit._tag).toBe("Failure");
    const defect = (exit as unknown as { cause: { reasons: { defect: Error }[] } }).cause.reasons[0]!.defect;
    expect(defect.message).toMatch(/has not published the DNS validation record for arn:aws:acm:us-west-2:1:certificate\/x/);
    expect(calls).toBe(3);
  });

  it("keeps certValidation as the override, refusing an empty one", () => {
    expect(validationRecordOverride({ name: "_a.x.com.", value: "_b.acm-validations.aws." })).toEqual({ name: "_a.x.com", value: "_b.acm-validations.aws" });
    expect(() => validationRecordOverride({ name: " ", value: "v" })).toThrow(/empty/);
  });

  it("ingress.ts no longer reads ResourceRecord with an empty-string fallback", () => {
    const src = readFileSync(fileURLToPath(new URL("../ingress.ts", import.meta.url)), "utf8");
    expect(src).not.toContain('ResourceRecord?.Name ?? ""');
    expect(src).toContain("waitForValidationRecord(");
    expect(src).toContain("validationRecordOverride(i.certValidation)");
  });
});
