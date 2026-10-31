/**
 * ACM's DNS validation record for the ingest certificate.
 *
 * ACM fills `DomainValidationOptions[].ResourceRecord` some seconds after
 * RequestCertificate returns, so the record captured at create time can be
 * empty, and an empty name reaches Cloudflare as "DNS name is invalid" (Stage A,
 * 2026-10-31). Without the `certValidation` input, the stack polls
 * DescribeCertificate (bounded) until the record is there and fails loudly if it
 * never appears: an empty record name is never emitted.
 */
import * as Effect from "effect/Effect";
import * as Duration from "effect/Duration";

export interface ValidationRecord {
  name: string;
  value: string;
}

interface DomainValidation {
  DomainName?: string;
  ResourceRecord?: { Name?: string; Type?: string; Value?: string };
}

const bare = (s: string) => s.replace(/\.$/, "");

/** The CNAME for `domain`, or undefined while ACM has not populated it. */
export function validationRecordFrom(options: DomainValidation[] | undefined, domain: string): ValidationRecord | undefined {
  const o = (options ?? []).find((d) => d.DomainName === domain) ?? options?.[0];
  const r = o?.ResourceRecord;
  if (!r?.Name?.trim() || !r.Value?.trim() || (r.Type && r.Type !== "CNAME")) return undefined;
  return { name: bare(r.Name.trim()), value: bare(r.Value.trim()) };
}

/** The `certValidation` override, normalized and refused if empty. */
export function validationRecordOverride(v: { name: string; value: string }): ValidationRecord {
  const r = { name: bare(v.name.trim()), value: bare(v.value.trim()) };
  if (!r.name || !r.value) throw new Error("96bulls: eks.ingress.certValidation has an empty name or value. Nothing was created.");
  return r;
}

export const VALIDATION_WAIT = { attempts: 60, interval: Duration.seconds(5) } as const;

/**
 * Poll `describe` until the record is populated. Dies (never returns an empty
 * record) after `attempts` tries; the error names the certificate.
 */
export function waitForValidationRecord(
  describe: () => DomainValidation[] | undefined,
  domain: string,
  certificateArn: string,
  wait: { attempts: number; interval: Duration.Duration } = VALIDATION_WAIT,
): Effect.Effect<ValidationRecord> {
  return Effect.gen(function* () {
    for (let n = 1; n <= wait.attempts; n++) {
      const r = validationRecordFrom(describe(), domain);
      if (r) return r;
      if (n < wait.attempts) yield* Effect.sleep(wait.interval);
    }
    return yield* Effect.die(
      new Error(`96bulls: ACM has not published the DNS validation record for ${certificateArn} (${domain}) after ${wait.attempts} checks; refusing to create an empty Cloudflare record. Retry, or pass eks.ingress.certValidation.`),
    );
  });
}
