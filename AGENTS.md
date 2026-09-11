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

At most one GitHub-mutating role or process may be active across all mutations
at a time; do not issue parallel mutations or create retry storms. Each role
may mutate only the GitHub resources within its stated authority. On a
confirmed GitHub rate-limit response, wait 2 minutes, resume from the failed
operation's checkpoint, and retry no more than 8 times. Report the operation if
it still cannot complete. Non-rate-limit errors are not eligible for this
retry policy; report and diagnose them normally.

## Planning workflow

1. `project_planner` reads the local blueprint and defines the work items in
   `docs/planning/v0.1-work-plan.yaml`.
2. The planner validates the draft plan; a human reviews and explicitly
   approves it.
3. `project_item_writer` preflights the approved plan and presents a mutation
   preview.
4. A human explicitly approves the preview.
5. `project_item_writer` creates/updates repository issues, links them to the Project, applies only the approved fields and relationships, then verifies and reports the result. Project-only draft issues are not work items for this workflow.

## Implementation workflow

1. The orchestrator assigns one unblocked Ready work item to a dedicated Git
   worktree and issue branch. Based on the dependency of work items, the orchestrator will assign the work to dedicated agents outlined below.
2. `coder` implements only that work item in the assigned worktree.
3. `tester` defines expected-behavior cases from the assigned issue and
   canonical blueprint before reading the implementation, then independently
   verifies the work. `task_reviewer` evaluates the completed task. `coder`,
   `tester`, and `task_reviewer` never stage, commit, or push.
4. After a `READY FOR REVIEW` verdict, standing authorization permits
   `pull_request_opener`—the only role allowed to stage, commit, or push—to
   publish the exact reviewed paths from the exact current worktree diff and
   open or update its PR without another human approval. It requires fresh
   tester evidence and `READY FOR REVIEW`, and must keep the same issue
   branch/worktree and PR.
5. After the PR is opened, the orchestrator performs one initial read, then
   polls its status via read-only `github` MCP operations no more frequently
   than once per minute, for a single 10-minute elapsed wall-clock cap per
   monitoring turn/window. Polls must cover CI checks, review completion,
   current-head review comments, approval, mergeability, and external
   auto-merge, always scoped to that PR's exact issue branch/worktree. Use
   actual check-run context names when evaluating required checks; do not infer
   them from workflow or job display labels.
   - Merged -> verify the PR is merged, its linked issue is `Closed`, and its
     Project status is `Done` when that field is configured. Report any status
     mismatch without mutating outside the responsible role's authority.
     Fast-forward local `main` to the merged remote state, then safely clean
     up that issue's worktree and branch.
   - Open with new current-head Copilot or human review comments requesting
     changes -> invoke `coder`, pinned to the same issue branch/worktree, to
     classify every comment as valid, invalid, or already addressed against
     the assigned issue and canonical blueprint. Change code only for valid
     findings; rejected findings require concrete repository, test, or
     authoritative-documentation evidence. Do not reopen user-rejected or
     resolved/outdated findings without new evidence. `coder` applies only the
     smallest accepted fixes and makes no unrelated changes. For review
     deltas, `tester` defines focused checks before inspecting the delta,
     independently verifies every `Valid` disposition, and produces fresh
     evidence before `task_reviewer` or publication. `task_reviewer` then
     re-checks it and reports an updated verdict without editing. After
     `READY FOR REVIEW`, `pull_request_opener` publishes the reviewed changes
     to the same branch and PR. Before requesting Copilot review, inspect the
     current-head SHA's reviews and checks; skip the request if one was already
     requested, is in progress, or is complete. Request exactly one Copilot
     review for each new head SHA; never duplicate a request for the same SHA.
   - At the 10-minute cap, stop polling and report the exact pending PR state
     for human follow-up. Do not extend the cap through additional rounds.
     The monitoring window starts with the initial read and is not reset by a
     push, agent restart, handoff, or context compaction. Only an explicit
     later user request to resume after stopping starts a new window.
   - Agents never merge PRs. GitHub/Copilot may approve and auto-merge them
     externally.
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
- Ordinary PR CI must complete end-to-end in under 5 minutes. Heavy full-
  repository scans, including NVD-backed scans, belong in scheduled or manual
  workflows with the blueprint-approved fast equivalent PR gates; moving a
  scan must not weaken required tests or security coverage.
- Local verification uses JDK 25 only. GitHub CI must explicitly provision JDK
  21 for the project's CI compatibility contract.
- After CI completes, calculate the PR critical-path elapsed time from the
  first relevant check start to the final required check completion. Treat
  5 minutes or longer as a workflow defect, and verify the CI workflow actually
  provisions JDK 21 rather than relying on the runner default.
