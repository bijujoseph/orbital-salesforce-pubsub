# GitHub Work Plan Schema

This repository uses this YAML contract for project planning.

Every entry under `issues` represents a real issue in the repository declared
by `project.repository`, linked into the declared GitHub Project. Project-only
draft issues are not valid work-plan outputs.

## Required target and approval contract

Every plan must contain a `project` mapping with non-empty `owner` and
`repository` strings and a positive integer `project_number`. The repository
must use the `owner/name` form, and its owner segment must equal
`project.owner`. These fields are the canonical target used by the writer;
they must not be inferred from agent instructions or chat context.

Every plan must contain an `approval` mapping with `approved`, `approved_by`,
and `approved_at` fields. A planner creates a draft with `approved: false` and
empty approval metadata. GitHub writes require `approved: true` plus non-empty
`approved_by` and `approved_at`; the writer validator enforces this gate with
`--require-approval`.

The target values in the example are illustrative placeholders. The planner
must replace them with the repository and Project identity it verifies for the
generated work plan.

```yaml
schema_version: "1.0"
item_kind: "repository_issue"

project:
  owner: "example-owner"
  repository: "example-owner/example-repository"
  project_number: 1
  target_version: "0.1.0"

approval:
  approved: false
  approved_by: ""
  approved_at: ""

defaults:
  status: "Backlog"
  priority: "P2"
  target_version: "0.1.0"
  agent_ready: "No"

required_project_fields:
  Status:
    - Backlog
    - Ready
    - In Progress
    - Ready for Review
    - Blocked
    - Done
  ItemType:
    - Epic
    - Feature
    - Task
    - Test
    - Documentation
    - Chore
    - Spike
  Priority:
    - P0
    - P1
    - P2
    - P3
  Area:
    - Foundation
    - Core
    - Subscription
    - Publish
    - CLI
    - Orbital Adapter
    - Testing
    - CI/CD
    - Documentation
    - Release
  Phase:
    - Phase 0
    - Phase 1
    - Phase 2
    - Phase 3
    - Phase 4
  Size:
    - XS
    - S
    - M
    - L
    - XL
  Risk:
    - Low
    - Medium
    - High
  Target version:
    - "0.1.0"
    - Future
  Agent-ready:
    - "Yes"
    - "No"

issues:
  - key: "EXAMPLE-001"
    type: "Epic"
    title: "Replace with a release title derived from the canonical blueprint"
    parent: null
    depends_on: []
    labels:
      - epic
      - example
    project_fields:
      Status: "Backlog"
      ItemType: "Epic"
      Priority: "P1"
      Area: "Core"
      Phase: "Phase 0"
      Size: "XL"
      Risk: "High"
      Target version: "0.1.0"
      Agent-ready: "No"
    body: |
      <!-- work-item-key: EXAMPLE-001 -->

      ## Goal
      Replace with a measurable goal derived from the canonical blueprint.

      ## Scope
      Replace with the applicable scope from the canonical blueprint.

      ## Requirement Sources
      Identify the applicable blueprint sections and any repository evidence
      supporting setup-specific engineering criteria.

      ## Implementation Notes
      Add concise implementation constraints.

      ## Acceptance Criteria
      - Add measurable completion criteria.

      ## Verification
      - Add test/build/manual verification commands.

      ## Risks / Constraints
      - Add relevant constraints.

      ## Likely Modules / Files
      - List likely impacted modules/files.
```

## Rules

- `item_kind` must be `repository_issue`; draft Project items are unsupported.
- `key` must be globally unique.
- `parent` must reference another plan key or be `null`.
- Every `depends_on` value must reference another plan key.
- Dependency cycles are invalid.
- Every issue body must include the marker:
  `<!-- work-item-key: <KEY> -->`.
- Every issue body must include non-empty Goal, Scope, Requirement Sources,
  Implementation Notes, Acceptance Criteria, Verification, Risks / Constraints,
  and Likely Modules / Files sections.
- Acceptance criteria must be testable pass/fail outcomes derived from the
  canonical blueprint and bounded to the issue. Verification evidence must map
  to those criteria.
- Setup and repository-preparation criteria may additionally use verified,
  stack-appropriate engineering practices. Requirement Sources must distinguish
  that evidence from blueprint-derived product requirements.
- New issues default to `Status: Backlog`.
