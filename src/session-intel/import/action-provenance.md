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
of trace paths occurs. Absolute POSIX and drive-letter paths are supported; a
drive path that resolves to its root is preserved as `X:/`; traversal above the
root is refused, rather than becoming a relative path or losing its drive
identity. Relative targets also resolve from POSIX `/` without becoming UNC
paths. UNC,
home expansion and paths without a resolvable base remain unknown.

The new `action-provenance.json` file is mode 0600 and SHA-pinned in the existing
snapshot manifest. It additionally pins the exact `sessions.jsonl` bytes, the
implementation sources, definition/bounds and retention
deadline. The private mapping beside the snapshot holds raw directory/target
paths, canonical repository claims and original call references. Those values
never appear in the hashed sidecar. Delete both exports and their reverse map
with the study by its earliest deletion deadline. Never commit real exports or
maps to this public repository.

The sidecar's `repositoryMapFingerprint` is a domain-separated HMAC of the map's
raw SHA-256, keyed with the private import salt. The unsalted map digest is kept
only in the private reverse map, preventing holders of a sidecar from confirming
a guessed map without the salt. Used claims' exact `observedAt` timestamps are
intentionally present as claim availability times: they are needed to avoid
future-evidence leakage and may disclose when an operator collected a map. The
hashed sidecar is not anonymous; review it before sharing. Record, directory,
target, repository-relative target and basename-hint identities use separate
HMAC domains, even if their untrusted input strings are identical.

`implementationSha256` pins the listed source files, not a complete executable
or dependency closure. It omits transitive files outside that list, installed
TypeScript/parser and other dependency versions, bundles, loaders, runtime and
platform configuration. Reproducible execution additionally needs the source
commit, lockfile, installed toolchain and those inputs. The default-off byte
golden covers the generic format; native adapters also have behavior tests, not
complete byte goldens for every native input shape.

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

## Version 2 repository paths (explicit capture opt-in)

`--action-repository-paths` also enables action provenance and emits
`session-intel-action-provenance/2` in the same `action-provenance.json` file.
Every record has a nullable `repositoryPath`; the definition names its encoding
as `m4-import-salted-segments/1`. This names the existing M4 import normalization,
not a new path form or scrub rule. Without this flag, both the normal import and
`--action-provenance`/`--repository-map` exports keep their previous behavior and
the latter keep the `/1` sidecar shape. A `/1` record cannot carry the new field;
mixed-format exports fail.

The field is emitted only for a write with an unambiguous repository claim,
an explicit matching operator-map root, and an individual target strictly inside
that root. Its root-relative path passes through the existing `repoRelativePath`
and `scrubPath` functions with the import's same Hasher and salt. Source/migration
directory names, migration numbers, known hot-path names and allowed extensions
use the existing closed vocabulary; other segments use the existing 12-hex HMAC.
For example, `supabase/migrations/0042_add_index.sql` keeps the migration number
as `supabase/migrations/0042_<12 hex>.sql`. The real T1 detector can read that
number; no replay adapter is introduced by this capture slice.

The opaque, case-preserving `repositoryTarget` remains the exact target identity.
The new path follows M4's lowercasing comparison policy, so it is semantic detector
input, not proof of exact filesystem identity. Different repository roots claiming
the same repository can produce the same path and target tokens, without probing
either checkout. A trace-only repository declaration without a map root does not
provide the new path. Missing/conflicting/projection scope, rejected paths and
commits/outcomes get null; unsafe paths and encoded paths over the existing
4,096-character path bound never bypass M4 guards. A mapped root that is a home
directory itself also gives null: M4's home-root anchoring could collapse distinct
subtrees and cannot establish this field's root-relative identity. A checkout
inside a home directory is still eligible under the existing path guards.
Availability still includes
the map's `observedAt`; the new path never backdates an action's evidence.

No new salt, key file or reverse-map file is introduced. The same private mapping
keeps the hashed path's normalized relative spelling in its existing session
`paths` map and the action's original target/context in `actionProvenance.locations`.
The existing mapping receipt binds the snapshot manifest, which pins the sidecar
and session bytes. The public sidecar uses M4's documented clear-name exceptions
and hashed segments; arbitrary path spellings stay in the private map. The
sidecar and private map retain their existing byte/count limits,
permissions and deletion deadline.

The current `/1` action-scope consumer deliberately refuses a `/2` sidecar rather
than silently interpreting or dropping the new field. A separately reviewed offline
per-repository detector adapter will consume it, preserving original session refs
and claim times. This export does not change production detectors, live capture,
claims, snapshots, replay, routes or feature gates.

The export is opt-in to preserve the existing CLI contract. Without it, the
snapshot and private mapping keep their previous shapes. With it, legacy session
bytes still do not change. The sidecar explicitly says `capture_only`,
`legacySessionScopeUnchanged: true` and `consumedByReplay: false`. The separately
opted-in descriptive patterns consumer supports `/1`; replay, snapshot types and
views will consume scope evidence in a separately ordered follow-up;
this slice does not repair their existing session-scope assumptions or establish
alert quality.

Capture is bounded to 100,000 provenance facts and 16 MiB of captured context,
with 16 MiB bounds on the sidecar
and additional private location map; static shell analysis allows 64 KiB and
4,096 tokens. An overflow fails before publishing an apparently complete export.
Use a smaller explicit frame rather than silently censoring evidence.
