# Security Policy

## Reporting a vulnerability

Please do not open a public issue for a suspected vulnerability or include
credentials, tokens, customer data, or a sensitive proof of concept in a
public discussion.

Use GitHub's private vulnerability reporting flow for this repository:

<https://github.com/bijujoseph/orbital-salesforce-pubsub/security/advisories/new>

You may also reach the same flow from the repository's **Security** tab by
selecting **Report a vulnerability**. GitHub may require you to sign in and
the repository must have private vulnerability reporting enabled. If the form
is unavailable, do not disclose the details publicly; ask a repository
maintainer to enable or provide a private reporting channel instead. This
project intentionally does not publish a security mailbox or private contact
address in the repository.

Include only the information needed to investigate the report:

- affected version, commit, module, or configuration;
- impact and realistic attack conditions;
- minimal reproduction steps or a safe proof of concept;
- relevant logs with secrets and personal data removed; and
- any suggested mitigation or disclosure constraints.

Do not test against systems or Salesforce orgs that you do not own or have
permission to assess. Maintainers will acknowledge reports and coordinate
follow-up through the private GitHub report when practical. Do not publish
unresolved details until a fix or coordinated disclosure decision is ready.

## Supported versions

Security fixes are intended for the current maintained release line. Once a
`0.1.x` release is published, use the latest patch release in that line. Older
or unreleased commits may not receive a backported fix; report the issue
against the exact commit or version you tested.

## Test-org credentials and rotation

Integration testing must use a dedicated, non-production Salesforce test org.
Production org credentials and customer data must never be used in local
development, fixtures, examples, CI, or logs.

Manage the test org's connected-app credentials, client secrets, refresh
tokens, and other authentication material through GitHub Actions encrypted
secrets or an approved secret manager. Keep secret values out of the
repository, issue tracker, build output, and diagnostic messages. Fixtures
must use synthetic values that cannot authenticate.

Rotate the dedicated test-org credentials:

1. Immediately after suspected exposure, accidental logging, or unauthorized
   access.
2. When ownership, maintainers, CI identities, runners, or secret-manager
   integrations change.
3. Before a protected release when the organization requires release-time
   rotation, and otherwise on the schedule required by the Salesforce org or
   the organization's security policy.

For every rotation, create replacement credentials in the dedicated test org,
update the approved secret store, verify the non-production integration suite,
revoke the previous credentials, and record the rotation in the appropriate
private operational audit trail. Confirm that old credentials no longer work
and that logs contain no secret values. If a credential was exposed, preserve
only the minimum private evidence needed for investigation and rotate before
resuming tests.

## Release and disclosure hygiene

Pull requests and scheduled workflows use the repository's configured secret,
code, dependency, and license checks. Release artifacts must not contain
secrets, private test-org data, or sensitive logs. Security reports and
remediation details remain private until the maintainers and reporter agree on
an appropriate disclosure path.

This policy does not change the project's license. Source and contributions
remain subject to the Apache License, Version 2.0; see [LICENSE](LICENSE) and
[NOTICE](NOTICE).
