import "../../packages/pipeline/src/lib/scrub-engine/hosted-lambda-grant.ts"; // FIRST import: module-load TS grant (docs/scrub-engine.md "Default flip")
export { handler, createReadApiLambdaHandler, readAppFromSecret, handleUrlEvent } from "../session-read-api/lambda.ts";
export * from "./composition.ts";
