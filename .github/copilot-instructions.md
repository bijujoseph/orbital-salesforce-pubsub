# GitHub Copilot Instructions

## Project Standards
- Scan `AGENTS.md` for project-wide standards before suggesting code or configs.
- Follow the conventions there unless explicit inline files override them.

## Pull Request Review Guidelines

### Scope of Review
- **Focus strictly on the specific work item** that the current Pull Request (PR) is addressing.
- Consult the linked issue, project work item details, and explicit acceptance criteria to guide your review bounds.
- **Do not make unnecessary or out-of-scope code suggestions.** Future work and alternative refactors will be handled in separate PRs in the pipeline.

### Dependabot Pull Requests
- When evaluating PRs generated automatically by Dependabot, your primary task is to check if the CI/CD and required action workflows pass.
- If you find a conflict, breaking change, or disagree with the dependency update, **add a clarifying comment explaining your reasoning and close the Dependabot PR**.
