// One Hono instance for the whole process: the vendored copy. Two copies would
// carry different private symbols and the vendored middleware could not read
// requests created by ours. Types come from hono-runtime.d.ts.
export * from "../../vendor/relayhistory-cloud/node_modules/hono/dist/index.js";
