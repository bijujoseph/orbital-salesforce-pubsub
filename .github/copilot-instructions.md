# GitHub Copilot Instructions

## Pull Request Reviews

- Consult `AGENTS.md` before reviewing and follow its repository-wide requirements; if these instructions conflict, `AGENTS.md` takes precedence.
- Inspect the complete pull request diff and every changed file before submitting the review.
- Report every distinct actionable finding discovered during that pass; do not intentionally defer a known finding to a later review.
- Consult the linked issue, Project work-item details, and explicit acceptance criteria when determining scope and expected behavior. Flag in-scope correctness, security, workflow, and regression-test gaps, but do not propose unrelated refactors.
- Do not reopen resolved, outdated, or user-rejected findings without new repository, test, or authoritative-documentation evidence.
- Make each finding specific and actionable, identify the affected code, and explain its impact.

## Dependabot Pull Requests

- Check compatibility with the build, runtime, and configuration; assess release and security implications; and inspect relevant tests and required workflow results.
- Report specific compatibility, security, or workflow findings. Do not suggest actions outside code review, such as closing the pull request.
