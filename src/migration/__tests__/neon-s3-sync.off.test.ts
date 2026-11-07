/** The pre-existing neon-s3-sync suite, run again with S3X_OBLIGATIONS OFF (the production default). */
process.env.S3X_OBLIGATIONS = "off";
await import("./neon-s3-sync.test.ts?mode=off");
