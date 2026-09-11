# Contributing

Thank you for helping improve the Orbital Salesforce Pub/Sub Connector. Please
use this guide for changes submitted to the repository.

## Before you start

1. Search existing issues and pull requests before opening a new one.
2. For a substantial change, open or comment on an issue so the proposed scope
   can be discussed before implementation.
3. Do not include credentials, customer data, access tokens, or other secrets
   in source code, fixtures, logs, documentation, or pull requests. Report a
   suspected vulnerability privately as described in [SECURITY.md](SECURITY.md).

## Development setup

The project targets Java 21. Repository-local verification uses JDK 25, while
CI explicitly provisions JDK 21 for compatibility checks. Maven is required for
the build.

Run the normal verification lifecycle from the repository root:

```sh
mvn -B -ntp clean verify -Ddependency-check.skip=true
```

The dependency scan is intentionally maintained as a scheduled or manual
security check because its vulnerability database can require a long update.
Changes that affect dependencies or security controls should also be checked
against the relevant workflow and configuration under `.github/`.

## Making changes

- Keep each pull request focused on one issue or a closely related change.
- Preserve the module boundaries and public API contracts documented by the
  project.
- Add or update tests for behavior changes. Do not weaken or remove a test to
  make the build pass.
- Keep documentation links and code examples accurate.
- Use synthetic event data and isolated non-production Salesforce resources for
  tests. Never use production credentials in local or CI verification.
- Check generated files and build output before committing so secrets and
  machine-specific files are not included.

## Pull requests

Use the repository pull request template. The description should explain the
problem, the change, and the verification performed, and should link the issue
being addressed. Reviewers must be able to understand any security, delivery,
compatibility, or operational impact from the description.

Before requesting review, confirm that:

- the focused tests and `mvn -B -ntp clean verify` pass with the required local
  JDK;
- Markdown, link, license, and secret checks pass where applicable;
- no credentials or sensitive payloads appear in the diff or test output; and
- the change is compatible with the Apache License, Version 2.0.

## Licensing

By intentionally submitting a contribution for inclusion, you agree that it
is provided under the terms of the Apache License, Version 2.0, unless a
separate written agreement with the project applies. See [LICENSE](LICENSE)
and [NOTICE](NOTICE).
