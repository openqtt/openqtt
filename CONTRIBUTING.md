# Contributing

You are welcome to submit bugs, issues and fixes on this repository. Everything
contributed is licensed under the Apache License 2.0; code from EMQX 5.9 or later
cannot be accepted, because it is not.

Anything that takes behaviour, logic or tests from another project follows the
porting policy in [ADR 0003](docs/adr/0003-porting-policy.md): implement from the
MQTT 5.0 specification, cite EMQX by permalink rather than paste it, mark a file
that translates EMQX logic in its header, and never copy Eclipse Paho test code.

## Before you open a pull request

- **Branch** from `main` as `<issue>/<slug>`, for example `12/codec-properties`.
  `release/N.x` is the only long-lived exception: `release/1.x` carries the 1.x
  line, and a fix for 1.x branches from it.
- **Run `make check`** and get it green. It is what CI runs.
- **No AI co-author trailers.** Do not add a `Co-authored-by` trailer naming a
  model or a tool, or a "generated with" footer, to a commit, a pull request or
  a release note.
- **No em dashes** in anything a reader sees. `make check` refuses them in
  Markdown.

## Commit Message Guidelines

We have very precise rules over how our git commit messages can be formatted. This leads to **more readable messages** that are easy to follow when looking through the **project history**.

### Commit Message Format

Each commit message consists of a **header**, a **body** and a **footer**. The header has a special format that includes a **type**, a **scope** and a **subject**:

```
<type>(<scope>): <subject>
<BLANK LINE>
<body>
<BLANK LINE>
<footer>
```

The **header** with **type** is mandatory. The **scope** of the header is optional. This repository has no predefined scopes. A custom scope can be used for clarity if desired; a crate's short name (`codec`, `session`, `edge`) is the usual one.

No line of the commit message may be longer than 100 characters. This allows the message to be easier to read on GitHub as well as in various git tools.

The footer should contain a [closing reference to an issue](https://help.github.com/articles/closing-issues-via-commit-messages/) if any.

Example 1:

```
feat(codec): decode the AUTH packet
```

Example 2:

```
fix(session): send DISCONNECT 0x8E to the old connection on takeover

Previously the old connection was closed without a reason code, so its client
could not tell a takeover from a network failure.

Closes: #123
```

### Revert

If the commit reverts a previous commit, it should begin with `revert: `, followed by the header of the reverted commit. In the body it should say: `This reverts commit <hash>.`, where the hash is the SHA of the commit being reverted.

### Type

Must be one of the following:

- **feat**: New feature for the user, not a new feature for build script
- **fix**: Bug fix for the user, not a fix to a build script
- **docs**: Documentation only changes
- **style**: Formatting, missing semi colons, etc; no production code change
- **refactor**: Refactoring production code, eg. renaming a variable
- **chore**: Maintenance that changes no production code
- **perf**: A code change that improves performance
- **test**: Adding missing tests, refactoring tests; no production code change
- **build**: Changes that affect the build system or external dependencies (example scopes: cargo, docker, makefile)
- **ci**: Changes to the CI workflows.
- **revert**: Reverts a previous commit.

### Scope

There are no predefined scopes for this repository. A custom scope can be provided for clarity.

### Subject

The subject contains a succinct description of the change:

- use the imperative, present tense: "change" not "changed" nor "changes"
- don't capitalize the first letter
- no dot (.) at the end

### Body

Just as in the **subject**, use the imperative, present tense: "change" not "changed" nor "changes". The body should include the motivation for the change and contrast this with previous behavior.

### Footer

The footer should contain any information about **Breaking Changes** and is also the place to reference GitHub issues that this commit **Closes**.

**Breaking Changes** should start with the word `BREAKING CHANGE:` with a space or two newlines. The rest of the commit message is then used for this.
