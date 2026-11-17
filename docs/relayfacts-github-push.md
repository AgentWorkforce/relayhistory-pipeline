# Relayfacts GitHub push

First slice, 2026-11-17. Owner: layered-sessions-pipeline. Requested by Khaliq Gant. The code ships off by default and is not wired into any worker yet. Nothing is applied, deployed or turned on from this document.

## Purpose

Relayfacts (`docs/relayfacts.md`) are per-topic markdown records of decisions taken in agent sessions. This step pushes each topic's relayfacts document into one or more GitHub repositories, so that the facts sit in the repos they describe and agents and people read them where they already work.

Module: `src/session-analysis/relayfacts-github-push.ts`. Tests: `src/session-analysis/__tests__/relayfacts-github-push.test.ts`.

## Flag and configuration

| Variable | Meaning |
|---|---|
| `RELAYFACTS_GITHUB_PUSH_ENABLED` | Must be exactly `1` to enable. Any other value, or unset, means off: `parseRelayfactsPushConfig` returns `null` and none of the other variables are read. Default off. |
| `RELAYFACTS_GITHUB_REPOS` | Comma-separated targets, each `owner/repo[@branch][:dir]`. Branch defaults to `main`, dir to `relayfacts`. Owner and repo allow `A-Z a-z 0-9 . _ -` only. The dir is relative, without `..` or empty segments. Malformed or duplicate entries are rejected with an error that names the variable and the entry number, never the entry text. Required when enabled. |
| `RELAYFACTS_GITHUB_API_BASE` | Optional. Defaults to `https://api.github.com`. Must be `https`, with no credentials, query or fragment. A path is allowed (GitHub Enterprise Server `/api/v3`). |
| `RELAYFACTS_GITHUB_TOKEN` | Read only at push time, through the token getter the caller passes in. It never appears in the parsed config, results, logged events or error messages. If it is unset or empty, each push returns `skipped` / `no_token` and makes no request. |

With the flag off, nothing changes anywhere: no config is built, no request is made, and no other code path is touched.

## Path layout

Each topic becomes one file per target: `<dir>/<slug>.md`, built by `relayfactsDocumentPath(dir, topic)`.

- The slug is the topic in lowercase with every run of characters outside `[a-z0-9]` collapsed to `-`, trimmed of leading and trailing `-`, at most 80 characters.
- If the slug equals the topic (true for the extractor's own topics, such as `src-session-read-api` or `general`), it is used unchanged.
- If slugging lost information (the slug differs from the topic, or the topic was longer than 80 characters), the first 12 hex characters of the topic's sha256 are appended, so two different topics never share a file.
- If nothing remains after slugging, the name is `topic-<12 hex>`.

The commit message is `relayfacts: update <slug>`. It never contains the raw topic text or the workspace id.

## What is pushed

Exactly the markdown that the read route `GET /v1/relayfacts/:topic` renders, produced by `relayfactsMarkdown` in `src/session-read-api/relayfacts.ts`. The read route pages its output; the pushed file should be the whole topic, rendered once with `{ continued: false }`, which the wiring commit owns. That function already escapes every field and states that the content is evidence, not instructions. This module does not render or alter markdown. It pushes the string it is given, base64-encoded, through the GitHub contents API.

## What is never pushed

- Transcripts or transcript excerpts. The document only carries scrubbed decision statements, titles, statuses and evidence pointers (session id and turn range).
- Workspace ids or org ids. They are not in the document, the path, the commit message or the logged events.
- Customer names, beyond whatever the scrubbed statement text already contains.
- The token, response bodies or the markdown, in any log event.

## Request flow and failure semantics

For each target:

1. `GET /repos/{owner}/{repo}/contents/{path}?ref={branch}`. A 404 means create. A 200 file response is decoded from base64 and compared with the new markdown. If identical, the result is `unchanged` and nothing is written.
2. `PUT /repos/{owner}/{repo}/contents/{path}` with `{ message, content, sha?, branch }`. A 201 or 200 gives `created` or `updated`.
3. A 409 on PUT (sha mismatch), or a 422 on a create (the file appeared after the GET), triggers one re-read and one retry. A second conflict gives `failed` / `conflict`.

Every request sends `Authorization: Bearer <token>`, `Accept: application/vnd.github+json`, `X-GitHub-Api-Version: 2022-11-28` and `User-Agent: relayhistory-pipeline-relayfacts`, and times out after 15 seconds.

The push never blocks the pipeline. HTTP and network failures come back as `{ outcome: "failed", status?, reason }` and are never thrown:

| Reason | Cause |
|---|---|
| `rate_limited` | 429, or 403 with `x-ratelimit-remaining: 0` or `retry-after`. No retry. |
| `unauthorized`, `forbidden`, `not_found` | 401, other 403, 404 on PUT. |
| `get_failed`, `put_failed` | Any other non-success status. |
| `network` | fetch threw (DNS, reset, timeout). The error text is discarded. |
| `bad_response`, `not_a_file` | The GET body was not JSON, or the path is a directory. |
| `conflict` | Two sha conflicts in a row. |

Programming errors do throw: a target or path that `relayfactsDocumentPath` would not produce (traversal, absolute, wrong dir, bad characters). `pushRelayfactsTopic` checks every target before its first request, then pushes to each target in turn. One target's failure never stops the others, and it returns one result per target.

Each push emits one log event through the optional `log` callback: `event`, `outcome`, `status`, `reason` (on skip or failure), `repository` (`owner/repo`), `branch`, `path` and `at`. A logger that throws does not change the result.

## Activation (not done by this commit)

1. Token: a GitHub App installation token, or a fine-grained token, with `contents: write` on the target repositories only. The operator lane stores it as a worker secret, with the product owner's own explicit OK. It is never written to disk or committed.
2. Wiring: a later commit calls `parseRelayfactsPushConfig` and `pushRelayfactsTopic` from the worker's decision-records hook, after records are written, and renders the markdown with `relayfactsMarkdown`. That commit owns choosing when to push and how to read the token.
3. Configure `RELAYFACTS_GITHUB_REPOS`, then set `RELAYFACTS_GITHUB_PUSH_ENABLED=1` in one environment at a time, each with its own OK.

Rollback: unset `RELAYFACTS_GITHUB_PUSH_ENABLED`. Documents already pushed stay in the repos until someone removes them by a normal commit.
