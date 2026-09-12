# Orbital Salesforce Pub/Sub Connector

## Product authority

- `.local/planning/orbital-salesforce-pubsub-final-blueprint-reviewed.md` is the
  sole authority for requirements, scope, architecture, and sequencing. It is
  intentionally Git-ignored. Read it; never edit, commit, publish, or quote
  substantial excerpts from it.
- The YAML plan, GitHub Issues, Project items, and PRs are derived artifacts.
  They may summarize but never alter the blueprint. Stop on any conflict until
  the applicable workflow corrects the derived artifact.
- README text, chat history, older plans, and generated issue text are not
  alternate requirements.

## GitHub operations

- Use the OAuth-authenticated `github` MCP for GitHub API operations. Never use
  PATs, `GH_TOKEN`, `gh`, raw HTTP/GraphQL, or credential-bearing scripts.
- Standard Git may publish through the existing authenticated remote. Never
  add or change Git credentials.
- Only `project_item_writer`, following `github-project-writing`, may mutate
  Issues or Project data.
- Allow only one GitHub-mutating role/process at a time. Each role stays within
  its stated authority; never run parallel mutations or retry storms.
- On a confirmed rate limit, wait 2 minutes, resume from the failed checkpoint,
  and retry at most 8 times. Report failure after the eighth retry. Do not apply
  this policy to other errors; diagnose and report them normally.

## Planning workflow

1. `project_planner` derives `docs/planning/v0.1-work-plan.yaml` from the local
   blueprint and validates it.
2. A human explicitly approves the plan.
3. `project_item_writer` preflights it and presents a mutation preview.
4. A human explicitly approves the preview.
5. `project_item_writer` applies and verifies only the approved Issues, Project
   fields, links, and relationships. Project-only drafts are not work items.

## Implementation workflow

1. Select one approved GitHub Issue that is unblocked and manually set to
   `Ready`. Assign it a dedicated issue branch, worktree, and `coder`. Never
   implement in the shared/default worktree.
2. Before editing, read the issue and applicable blueprint sections. The issue
   bounds execution; the blueprint governs requirements. Stop on ambiguity,
   missing prerequisites, or conflicts among blueprint, plan, issue, and code.
   Do not guess or widen scope. Apply every relevant architecture, API,
   security, delivery, test, and documentation rule from the blueprint.
3. `coder` implements only that issue. `tester` defines expected behavior from
   the issue and blueprint before reading the implementation, then verifies it
   independently. `task_reviewer` reviews the completed task. These roles never
   stage, commit, or push.
4. Fresh tester evidence plus `READY FOR REVIEW` authorizes
   `pull_request_opener`—the only role allowed to stage, commit, or push—to
   publish the exact reviewed current-worktree diff and open/update the same
   branch/worktree/PR. No additional human approval is required unless a
   current-head Copilot human-review escalation contains no concrete change or
   suggestion that the agents can implement.
5. After opening a PR, the orchestrator performs an initial read and monitors
   it with read-only `github` MCP calls at least 1 minute apart. Every poll must
   cover actual check-run contexts, review completion, unresolved current-head
   comments, approval, mergeability, and external auto-merge for the exact
   issue branch/worktree. Never infer check contexts from workflow/job labels.
   - **Merged:** verify the PR is merged, its issue is `Closed`, and configured
     Project status is `Done`. Report mismatches without exceeding role
     authority. Fast-forward local `main`, then safely remove that issue's
     worktree and branch.
   - **Actionable current-head review:** inspect current-head review bodies,
     summaries, suppressed findings, and review threads; a successful review
     check means the automation completed, not that the PR was approved.
     Copilot recommendations may be accepted or disagreed with. No explorer
     step is required when agents accept and will implement a recommendation.
     If `coder` intends
     to classify a Copilot recommendation as `Invalid` or `Already addressed`,
     do not communicate that disagreement yet. First invoke `explorer` to trace
     the finding against the affected code paths, tests, issue, blueprint, and
     authoritative documentation when needed, then give that evidence to
     `coder` for reconsideration. If explorer supports Copilot or evidence is
     inconclusive, classify finding `Valid` and implement it.
     Disagreement is allowed only when the explorer finds clear contrary
     evidence, `coder` addresses it, and `task_reviewer` accepts it. Invoke
     `coder` in the same worktree. Classify every eligible finding as `Valid`,
     `Invalid`, or `Already addressed` against the issue and blueprint. Change
     code only for `Valid` findings. Support rejections with repository, test,
     or authoritative documentation evidence, including explorer handoff.
     Ignore resolved/outdated comments; do not reopen user-rejected findings
     without new evidence. Apply only minimal fixes.
     Before inspecting the delta, `tester` defines focused checks; it then
     verifies every `Valid` disposition and produces fresh evidence.
     `task_reviewer` re-reviews without editing. After `READY FOR REVIEW`,
     `pull_request_opener` publishes to the same branch/PR.
   - **Copilot human-review escalation:** a blue `Needs a closer look` outcome
     overrides normal agent-disagreement handling, even without an inline
     thread. Classify every concrete change or suggestion included in that
     escalation as `Valid (human-review-escalated)`. Do not disagree, classify
     it `Already addressed`, or invoke `explorer` to challenge it. `coder` must
     implement the smallest in-scope fix, `tester` must run focused regression
     coverage, and `task_reviewer` must approve the exact delta. After those
     gates pass, `pull_request_opener` must commit and push the reviewed delta
     to the same PR without additional human approval, then trigger one
     rereview for the new head. Bind this action
     to exact PR number, Copilot review ID, and reviewed head SHA. Passing checks
     or resolved/outdated threads do not replace this round. If blue outcome
     contains no concrete implementable change or suggestion, block `READY FOR
     REVIEW`, publication, and automated merge readiness until human explicitly
     accepts or redirects risk.
   - **Copilot review:** before requesting one, inspect reviews and checks for
     the current head SHA. Skip if already requested, running, or complete.
     After a reviewed push, request exactly once for the new head SHA when an
     automatic review is not already queued, running, or complete.
   - **Rolling window:** the initial read starts a 10-minute window. Actionable
     comments received before expiry authorize that full fix/test/review/publish
     round, subject to the hard cap. A reviewed push is the only reset and
     starts a new 10-minute window for the new head. Polls, retries, restarts,
     handoffs, and context compaction do not reset it. If it expires with no
     review round active and no new reviewed push, stop and report exact state.
   - **Hard cap:** the initial read also starts a 120-minute cap for the complete
     review/fix/publish exchange. Nothing resets it. At expiry, stop and report
     exact head, CI, review, approval, mergeability, and merge state. Only an
     explicit later request to resume starts new 120- and 10-minute clocks.
   - Agents never merge. GitHub/Copilot may approve and auto-merge externally.
6. `final_reviewer` evaluates milestone or release readiness across completed
   tasks.

## Quality gates

- Keep each PR to one issue; include its key in branch and PR title.
- Run every verification required by the issue and applicable blueprint.
- Never weaken, skip, or delete tests to pass a build.
- Ordinary PR CI must finish end-to-end in under 5 minutes. Put heavy full-repo
  scans, including NVD-backed scans, in scheduled/manual workflows with the
  blueprint-approved fast PR equivalents. Never reduce security or test
  coverage when moving a scan.
- Use JDK 25 for local verification. CI must explicitly provision JDK 21.
- After CI, calculate critical-path time from the first relevant check start to
  the last required check completion. Treat 5 minutes or more as a workflow
  defect. Verify CI uses provisioned JDK 21, not the runner default.
