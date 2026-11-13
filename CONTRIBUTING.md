# Contributing

Thanks for your interest in The Agent Relay sessions pipeline. Bug reports, fixes and ideas are
welcome.

## Proposing a change

Pull requests will be switched off on this repository; once they are, it takes
none. Until then, fork pull request CI runs without repository secrets:
workflows that use secrets (publishing and deploys) run only from `main`, from
release tags, or when a maintainer with write access starts them. Either way,
the way to propose a change is an issue. Open a
[GitHub issue](https://github.com/AgentWorkforce/relayhistory-pipeline/issues) instead:

- **Security issues:** do not open a public issue. Follow [SECURITY.md](SECURITY.md).
- **Bugs:** the version or commit, what you expected, what happened, and the smallest
  reproduction you can manage. Remove credentials, tokens and personal data from logs before
  you paste them.
- **Features:** the problem you want solved and how you would use it.
- **A fix you already have:** describe it in the issue, and include the diff if it is short.
  The maintainers make the change, put it through review and CI, and credit you in the commit.

Never include secrets, credentials, real customer data or production identifiers in an issue,
including in diffs and fixtures. Use obviously fake values.

## Development

```sh
git submodule update --init
bun install --frozen-lockfile
bun run typecheck
bun run test
bun run e2e:pipeline     # the full-pipeline e2e on a fresh local stack (needs Docker)
```

The maintainers' CI is a flow (`flows/ci.flow.ts`) that runs these and more; see
[docs/maintainer-workflow.md](docs/maintainer-workflow.md) and
[docs/pipeline-e2e.md](docs/pipeline-e2e.md). The rules in [AGENTS.md](AGENTS.md) apply to
every change in this repository.

## License

By contributing, you agree that your contributions are licensed under the
[Apache License 2.0](LICENSE), the license of this repository.
