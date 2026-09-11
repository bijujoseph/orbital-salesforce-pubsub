# Orbital Salesforce Pub/Sub Connector

## Canonical product source of truth

The only authoritative source for product requirements, release scope,
architecture, and implementation sequencing is:

.local/planning/orbital-salesforce-pubsub-final-blueprint-reviewed.md

This file is intentionally ignored by Git.

Agents may read it locally. Agents must not edit, commit, or publish the
blueprint itself or substantial verbatim excerpts from it.

The YAML work plan, GitHub Issues, Project items, and pull requests are derived
execution and tracking artifacts. They may summarize the blueprint but must
not add, remove, or override its requirements. If a derived artifact conflicts
with the blueprint, stop and report the conflict. Work may resume only after
the derived artifact is corrected through its applicable workflow.

Do not use README text, old chat history, older planning documents, generated
issue text, or prior plans as alternate product requirements.

## GitHub operations

Use the OAuth-authenticated `github` MCP for GitHub API operations; do not use
PATs, `GH_TOKEN`, `gh`, raw HTTP/GraphQL calls, or credential-bearing scripts.
Standard Git commands may publish an issue branch through the repository's
existing authenticated remote; never add or change Git credentials.
Only `project_item_writer`, following the `github-project-writing` skill, may
mutate GitHub Issues or Project data.

## Planning workflow

1. `project_planner` reads the local blueprint and defines the work items in
   `docs/planning/v0.1-work-plan.yaml`.
2. The planner validates the draft plan; a human reviews and explicitly
   approves it.
3. `project_item_writer` preflights the approved plan and presents a mutation
   preview.
4. A human explicitly approves the preview.
5. The writer creates repository issues, links them to the Project, applies
   only the approved fields and relationships, then verifies and reports the
   result. Project-only draft issues are not work items for this workflow.

## Implementation workflow

1. The orchestrator assigns one unblocked Ready work item to a dedicated Git
   worktree and issue branch.
2. `coder` implements only that work item in the assigned worktree.
3. `tester` independently verifies it, then `task_reviewer` evaluates the
   completed task.
4. After a `READY FOR REVIEW` verdict, `pull_request_opener` publishes the issue
   branch and opens its pull request.
5. After the PR is opened, the orchestrator polls its status via read-only
   `github` MCP operations every 1 minute, up to 10 minutes per round, always
   scoped to that PR's exact issue branch/worktree:
   - Merged -> proceed to worktree cleanup for that issue's branch/worktree.
   - Open with new Copilot (or human) review comments requesting changes ->
     invoke `coder`, pinned to that same issue branch/worktree, to triage each
     comment against the assigned issue and canonical blueprint. `coder` may
     reject a comment with stated rationale if it disagrees. For comments it
     accepts, `coder` applies the smallest fix, re-runs required verification,
     commits, and pushes the same branch. `task_reviewer` then re-checks the
     updated diff and reports an updated verdict; it must not edit anything
     itself. Start a new 10-minute poll round after the push.
   - Open with no new comments after 10 minutes -> stop, report the pending
     PR for human follow-up.
   - After 3 poll/fix rounds (up to ~30 minutes total) -> stop regardless of
     outcome and report the PR for human follow-up.
6. `final_reviewer` evaluates milestone or release readiness across completed
   tasks.

## Implementation authority

- Work on exactly one approved GitHub Issue that is unblocked and manually
  moved to Ready.
- Work only in the branch and worktree assigned to that issue. Do not switch or
  reuse the shared/default worktree for implementation.
- Read the assigned issue and the relevant canonical-blueprint sections before
  implementation.
- Treat the issue as a bounded execution unit derived from the blueprint, not
  as authority to change product requirements.
- Stop and report any conflict between the issue, work plan, implementation,
  and blueprint. Do not resolve product ambiguity by guessing.
- Follow all architecture, API, security, delivery-semantics, testing, and
  documentation requirements in the blueprint that apply to the assigned work.

## Quality rules

- Keep each PR focused on one GitHub issue.
- Use the issue key in the branch and PR title.
- Run every verification required by the assigned issue and the applicable
  canonical-blueprint sections.
- Never weaken, skip, or delete tests just to obtain a passing build.
