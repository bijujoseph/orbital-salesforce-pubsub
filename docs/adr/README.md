# Architecture decision records

Architecture decision records (ADRs) capture durable choices that affect the
project's structure, public behavior, security, operations, or compatibility.
They are concise records of decisions, not implementation diaries or a copy of
private planning material.

## File and numbering convention

- Name records `NNNN-short-title.md`, using four-digit, zero-padded numbers.
- Use lowercase kebab-case for the short title.
- Allocate the next number when a record is started; do not reuse a number.
- Keep the record in this directory and link it from related documentation or
  pull requests when useful.
- Start from the reserved template [000-template.md](000-template.md); it is
  the only three-digit filename exception and is not an ADR record.

## When to write an ADR

Write an ADR when a choice is difficult to reverse, affects more than one
module, changes a public contract, introduces a security or operational
trade-off, or resolves an important compatibility constraint. Small local
implementation choices belong in code and tests instead.

## Required content

Every ADR should include:

- status and date;
- the problem or context that required a decision;
- the decision and its boundaries;
- alternatives considered and why they were not selected;
- consequences, including compatibility and operational impact; and
- security, testing, and follow-up considerations when relevant.

Use one of these statuses: `Proposed`, `Accepted`, `Superseded`, or
`Deprecated`. Link a superseding ADR when one replaces an earlier decision.

## Writing and review rules

- Describe the decision in the project's own words; never copy or publish the
  ignored canonical blueprint or other private planning content.
- Do not include secrets, credentials, private customer information, or raw
  sensitive payloads.
- Prefer stable rationale and constraints over transient task details.
- Review an ADR with the code or configuration change it governs.
- Update the ADR when the decision changes, and preserve the original record
  for historical context.
