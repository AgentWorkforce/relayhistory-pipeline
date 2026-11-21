# Import action provenance (capture-only)

A session can work in several repositories. Its session-level `repo` is not an
independent repository attribution for every edit, commit or revert. This optional
export records the directory evidence for each such fact without changing the
legacy snapshot format or feeding a deployed detector.

Run the existing import CLI with `--action-provenance`. An optional
`--repository-map <private-file>` also enables the export. The normal input,
output, salt and deletion-deadline options remain required. Pipe the salt through
stdin; no new credential or Git access is needed.

A repository map is a private, regular, non-symlink file (0600), outside the
repository, at most 2 MiB and 10,000 roots:

```json
{
  "format": "session-intel-repository-map/1",
  "roots": [{
    "directory": "/work/api-checkout",
    "repository": "git.example/acme/api",
    "observedAt": "2026-11-01T09:00:00Z"
  }]
}
```

These entries are operator declarations. The importer reads this explicit file;
it never runs Git, fetches, reads environment credentials, probes its roots or
opens any directory named by a trace. A future `observedAt`, duplicate normalized
root, malformed identity, credential-bearing URL or over-budget input is refused
without echoing values. The most specific lexical root wins. Case and symlinks
are not guessed: path identity preserves case and no filesystem canonicalization
of trace paths occurs. Absolute POSIX and drive-letter paths are supported; UNC,
home expansion and paths without a resolvable base remain unknown.

The new `action-provenance.json` file is mode 0600 and SHA-pinned in the existing
snapshot manifest. It additionally pins the exact `sessions.jsonl` bytes, the
implementation sources, definition/bounds, repository-map bytes and retention
deadline. The private mapping beside the snapshot holds raw directory/target
paths, canonical repository claims and original call references. Those values
never appear in the hashed sidecar. Delete both exports and their reverse map
with the study by its earliest deletion deadline. Never commit real exports or
maps to this public repository.

Each record names the legacy harness/session/event id and fact kind, with:

- the fact time and source-context availability time;
- salted starting/effective directory and target identities;
- the source and a fixed reason when scope could not be established;
- a salted repository claim, its basis and availability time, or null;
- an exact-case, repository-relative target identity when an explicit map root
  exists, so matching files in different checkouts can share an identity;
- a basename hint, when possible, that is never promoted to a repository id.

Canonical claims retain the host and full repository path. They do not share the
legacy basename-only repository identity space. `operator_declared` means an
explicit map matched the effective path. `trace_declared` means an explicit
per-event declaration or matching session metadata was available at capture time.
Neither means independently verified. When applicable trace and operator claims
disagree, both are retained as claims, the record is marked `conflicting_claims`
and no canonical repository or repository-relative target is selected. A later map or trace declaration cannot
make a record available earlier: `availableAt` is no earlier than its fact,
source context or the claim used. Consumers must honor that timestamp.

The adapters retain call-time context before matching commit output. The SHA
still points to its output line and availability time; location evidence points
to the original call and preserves its paired output reference. Claude legacy
commit timestamps remain unchanged, but sidecar availability waits for the SHA
output there too. An explicit generic per-event repository claim can exist without a directory;
no filesystem location is inferred from it. Such declarations do not propagate
to other events. Missing legacy contexts stay unattributed; they never inherit the final
session repository value. Absolute file targets can match a different root than
the starting directory.

Static Git directory analysis supports one target action, literal `git -C`
chains and literal leading `cd <path> &&` with Git commands. Pipes, substitutions,
redirection, globbing, unsupported prefixes/configuration and ambiguous actions
leave scope unknown. Shell-patch and code-mode projection writes with no safe
per-action association also remain unknown. Dynamic or unsupported workdir
fields do not fall back to the session directory. Unknown shell flow can retain
its starting-directory evidence without asserting the Git action's repository.
Arbitrary runtime effects, aliases, shell initialization and historical symlink
changes are not proved by this static analysis. This is evidence about intent,
not filesystem success, authorship, merge state or harmful overlap.

The export is opt-in to preserve the existing CLI contract. Without it, the
snapshot and private mapping keep their previous shapes. With it, legacy session
bytes still do not change. The sidecar explicitly says `capture_only`,
`legacySessionScopeUnchanged: true` and `consumedByReplay: false`. Replay, snapshot
types and views will consume this evidence in the separately ordered follow-up;
this slice does not repair their existing session-scope assumptions or establish
alert quality.

Capture is bounded to 100,000 provenance facts and 16 MiB of captured context,
with 16 MiB bounds on the sidecar
and additional private location map; static shell analysis allows 64 KiB and
4,096 tokens. An overflow fails before publishing an apparently complete export.
Use a smaller explicit frame rather than silently censoring evidence.
