/**
 * #135's ClickHouse suite reads its server from process.env at module scope
 * (src/session-read-api/__tests__/support.ts). Inside workerd those values
 * come from this run's bindings (vitest.config.ts), set before any suite loads.
 */
import { env } from "cloudflare:workers";
import { declareScrubTsCapability } from "../../pipeline/src/lib/scrub-engine/ts-capability.ts";

// The deployed Worker declares "hosted-worker" at fetch entry; suites that call the app
// below the fetch handler keep TypeScript scrub through this test grant
// (docs/scrub-engine.md "Default flip").
declareScrubTsCapability("test");

const e = env as unknown as Record<string, string>;
process.env.READ_API_TEST_CLICKHOUSE_URL = e.CLICKHOUSE_URL;
process.env.READ_API_TEST_CLICKHOUSE_USER = e.TEST_CH_ADMIN_USER;
process.env.READ_API_TEST_CLICKHOUSE_PASSWORD = e.TEST_CH_ADMIN_PASSWORD;
