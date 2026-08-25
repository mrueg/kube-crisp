<!--
  Thanks for the pull request. CONTRIBUTING.md has the longer version of this;
  the short version is below.
-->

## What this changes

<!-- What it does, and why. If it fixes an issue, "Fixes #123". -->

## How it was verified

<!--
  For a bug fix, the useful thing is that the test fails without the change and
  passes with it — say so, and say how you checked. For a performance change,
  the numbers and what produced them.
-->

## Checklist

- [ ] `make verify` and `make lint` pass
- [ ] `make cover` passes — the race detector matters here: the watch cache is
      refreshed while watchers read from it, and the served API surface is
      swapped under live requests
- [ ] Behaviour changes are covered by a test that fails without them
- [ ] API type changes are regenerated (`make codegen`)
- [ ] Documentation is updated where the behaviour it describes has changed
- [ ] Commits follow [Conventional Commits](https://www.conventionalcommits.org),
      since the release notes are generated from them
