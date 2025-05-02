# Contributing to the Oracle NoSQL Database

*Copyright (c) 2024, 2025 Oracle and/or its affiliates. All rights reserved.*

The target readers of this document are those who want to contribute to the
Oracle NoSQL Database, including but not limited to contributing
to the source code, tests and documents.

## Before contributing

### Sign the OCA

Before you become a contributor, please read and sign
[Oracle Contributor Agreement][OCA](OCA),
see [Contributing](./CONTRIBUTING.md) for more details.

After you signed the OCA, make sure that your Git tool is configured to create
commits using the name and e-mail address you used to sign the OCA:
You can configure Git globally (or locally as you prefer) with the commands:

```bash
git config --global user.email you@example.com
git config --global user.name YourName
```

### Check the issue tracker

When you find any issues with the Rust SDK or want to propose a change, please
check the [Issues](./issues) page first, this helps prevent duplication of
effort. If the issue is already being
tracked, feel free to participate in the discussion.

### Opening issues

If you find an issue that is not tracked in the [Issues](./issues)
page, feel free to open a new one, describe the issue, discuss your plans or
proposed changes.
All contributions should be connected to an issue except for the trivial changes.

## Contributing Code

Follow the [Github Flow](https://guides.github.com/introduction/flow/) when you
work on a change for Rust SDK.

Before you open a pull request, make sure:

- Add unit tests for the code changes you made.
- Run all tests.
  - It is important to run all tests and make sure they pass.

## Run Tests

Refer to [README](./README.md) Running tests section.

## Pull Request Process

Pull requests can be made under
[Oracle Contributor Agreement][OCA](OCA).

For pull requests to be accepted, the bottom of
your commit message must have the following line using your name and
e-mail address as it appears in the OCA Signatories list.

```text
Signed-off-by: Your Name <you@example.org>
```

This can be automatically added to pull requests by committing with:

```bash
git commit --signoff
```

Only pull requests from committers that can be verified as having
signed the OCA can be accepted.

## Code of conduct

Follow the [Golden Rule](https://en.wikipedia.org/wiki/Golden_Rule). If you'd
like more specific guidelines, see the [Contributor Covenant Code of Conduct][COC].

[OCA]: https://oca.opensource.oracle.com
[COC]: https://www.contributor-covenant.org/version/1/4/code-of-conduct/
