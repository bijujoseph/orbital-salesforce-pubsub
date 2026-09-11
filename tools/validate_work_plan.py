#!/usr/bin/env python3
"""Validate the repository's machine-readable GitHub work-plan contract."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Any

import yaml


SCHEMA_VERSION = "1.0"
SUPPORTED_ISSUE_TYPES = (
    "Epic",
    "Feature",
    "Task",
    "Test",
    "Documentation",
    "Chore",
    "Spike",
)

# These are the fields and options documented by docs/planning/work-plan-schema.md.
# The values are checked both in the declaration and when an issue uses a field.
SUPPORTED_PROJECT_FIELD_OPTIONS = {
    "Status": (
        "Backlog",
        "Ready",
        "In Progress",
        "Ready for Review",
        "Blocked",
        "Done",
    ),
    "ItemType": SUPPORTED_ISSUE_TYPES,
    "Priority": ("P0", "P1", "P2", "P3"),
    "Area": (
        "Foundation",
        "Core",
        "Subscription",
        "Publish",
        "CLI",
        "Orbital Adapter",
        "Testing",
        "CI/CD",
        "Documentation",
        "Release",
    ),
    "Phase": ("Phase 0", "Phase 1", "Phase 2", "Phase 3", "Phase 4"),
    "Size": ("XS", "S", "M", "L", "XL"),
    "Risk": ("Low", "Medium", "High"),
    "Target version": ("0.1.0", "Future"),
    "Agent-ready": ("Yes", "No"),
}

REQUIRED_DEFAULTS = {
    "status": "Status",
    "priority": "Priority",
    "target_version": "Target version",
    "agent_ready": "Agent-ready",
}

REQUIRED_BODY_SECTIONS = (
    "Goal",
    "Scope",
    "Requirement Sources",
    "Implementation Notes",
    "Acceptance Criteria",
    "Verification",
    "Risks / Constraints",
    "Likely Modules / Files",
)


def fail(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    raise SystemExit(1)


class StrictSafeLoader(yaml.SafeLoader):
    """SafeLoader variant that rejects duplicate or non-hashable mapping keys."""

    def construct_mapping(
        self, node: yaml.MappingNode, deep: bool = False
    ) -> dict[Any, Any]:
        if not isinstance(node, yaml.MappingNode):
            raise yaml.constructor.ConstructorError(
                None,
                None,
                "expected a mapping node",
                node.start_mark,
            )

        mapping: dict[Any, Any] = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            try:
                duplicate = key in mapping
            except TypeError as exc:
                raise yaml.constructor.ConstructorError(
                    "while constructing a mapping",
                    node.start_mark,
                    "found an unhashable key",
                    key_node.start_mark,
                ) from exc
            if duplicate:
                raise yaml.constructor.ConstructorError(
                    "while constructing a mapping",
                    node.start_mark,
                    f"found duplicate key ({key!r})",
                    key_node.start_mark,
                )
            mapping[key] = self.construct_object(value_node, deep=deep)
        return mapping


def load_plan(path: Path) -> Any:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return yaml.load(handle, Loader=StrictSafeLoader)
    except yaml.YAMLError as exc:
        fail(f"Invalid YAML in {path}: {exc}")
    except (OSError, UnicodeError) as exc:
        fail(f"Unable to read plan {path}: {exc}")


def require_mapping(value: Any, path: str) -> dict[Any, Any]:
    if not isinstance(value, dict):
        fail(f"{path} must be a mapping")
    for key in value:
        if not isinstance(key, str):
            fail(f"{path} keys must be strings")
    return value


def require_string(value: Any, path: str, *, non_empty: bool = True) -> str:
    if not isinstance(value, str):
        fail(f"{path} must be a string")
    if non_empty and not value.strip():
        fail(f"{path} must be a non-empty string")
    return value


def require_clean_string(value: Any, path: str) -> str:
    result = require_string(value, path)
    if result != result.strip():
        fail(f"{path} must not have leading or trailing whitespace")
    return result


def require_string_list(value: Any, path: str) -> list[str]:
    if not isinstance(value, list):
        fail(f"{path} must be a list")

    result: list[str] = []
    for index, item in enumerate(value):
        result.append(require_clean_string(item, f"{path}[{index}]"))
    if len(result) != len(set(result)):
        fail(f"{path} must not contain duplicate values")
    return result


def validate_project(plan: dict[Any, Any]) -> dict[str, Any]:
    project = require_mapping(plan.get("project"), "project")

    owner = require_clean_string(project.get("owner"), "project.owner")
    repository = require_clean_string(project.get("repository"), "project.repository")
    repository_parts = repository.split("/")
    if len(repository_parts) != 2 or not all(repository_parts):
        fail("project.repository must use the non-empty owner/name form")
    if repository_parts[0] != owner:
        fail("project.repository owner must match project.owner")

    project_number = project.get("project_number")
    if (
        isinstance(project_number, bool)
        or not isinstance(project_number, int)
        or project_number <= 0
    ):
        fail("project.project_number must be a positive integer")

    if "target_version" in project:
        require_clean_string(project["target_version"], "project.target_version")

    return project


def validate_required_project_fields(plan: dict[Any, Any]) -> dict[str, list[str]]:
    declared = require_mapping(
        plan.get("required_project_fields"), "required_project_fields"
    )

    missing = [
        field for field in SUPPORTED_PROJECT_FIELD_OPTIONS if field not in declared
    ]
    if missing:
        fail(
            "required_project_fields is missing required fields: "
            + ", ".join(missing)
        )
    extra = sorted(set(declared) - set(SUPPORTED_PROJECT_FIELD_OPTIONS))
    if extra:
        fail(
            "required_project_fields contains unsupported fields: "
            + ", ".join(extra)
        )

    result: dict[str, list[str]] = {}
    for field, allowed_options in SUPPORTED_PROJECT_FIELD_OPTIONS.items():
        options = require_string_list(
            declared[field], f"required_project_fields.{field}"
        )
        if not options:
            fail(f"required_project_fields.{field} must not be empty")
        unsupported = sorted(set(options) - set(allowed_options))
        if unsupported:
            fail(
                f"required_project_fields.{field} contains unsupported option(s): "
                + ", ".join(unsupported)
            )
        result[field] = options

    if "Backlog" not in result["Status"]:
        fail("required_project_fields.Status must include Backlog")
    return result


def validate_defaults(
    plan: dict[Any, Any], project_fields: dict[str, list[str]]
) -> None:
    defaults = require_mapping(plan.get("defaults"), "defaults")
    for default_name, project_field in REQUIRED_DEFAULTS.items():
        value = require_clean_string(
            defaults.get(default_name), f"defaults.{default_name}"
        )
        if value not in project_fields[project_field]:
            fail(
                f"defaults.{default_name} has unsupported value {value!r} for "
                f"{project_field}"
            )

    if defaults["status"] != "Backlog":
        fail("defaults.status must be Backlog")


def validate_approval(plan: dict[Any, Any], require_approval: bool) -> None:
    approval = require_mapping(plan.get("approval"), "approval")
    approved = approval.get("approved")
    if not isinstance(approved, bool):
        fail("approval.approved must be a boolean")

    metadata: dict[str, str] = {}
    for field in ("approved_by", "approved_at"):
        metadata[field] = require_string(
            approval.get(field), f"approval.{field}", non_empty=False
        )

    if require_approval and approved is not True:
        fail("approval.approved must be true when --require-approval is used")

    if approved:
        for field, value in metadata.items():
            if not value.strip():
                fail(f"approval.{field} must be non-empty for an approved plan")


def validate_issue_fields(
    issue: dict[Any, Any],
    index: int,
    project_fields: dict[str, list[str]],
) -> tuple[str, str, str | None, list[str]]:
    prefix = f"issues[{index}]"
    key = require_clean_string(issue.get("key"), f"{prefix}.key")

    issue_type = require_clean_string(issue.get("type"), f"{key}.type")
    if issue_type not in SUPPORTED_ISSUE_TYPES:
        fail(
            f"{key}.type must be one of: "
            + ", ".join(SUPPORTED_ISSUE_TYPES)
        )
    require_clean_string(issue.get("title"), f"{key}.title")

    body = require_string(issue.get("body"), f"{key}.body")
    marker = f"<!-- work-item-key: {key} -->"
    if marker not in body:
        fail(f"{key}.body must include marker: {marker}")
    headings = list(re.finditer(r"(?m)^##[ \t]+.+?[ \t]*$", body))
    for section in REQUIRED_BODY_SECTIONS:
        heading = f"## {section}"
        section_start = next(
            (
                match
                for match in headings
                if match.group(0).rstrip() == heading
            ),
            None,
        )
        if section_start is None:
            fail(f"{key}.body must include section: {heading}")
        content_start = section_start.end()
        next_heading = next(
            (match.start() for match in headings if match.start() > content_start),
            len(body),
        )
        section_content = body[content_start:next_heading]
        if not section_content.strip():
            fail(f"{key}.body section must not be empty: {heading}")

    labels = issue.get("labels", [])
    require_string_list(labels, f"{key}.labels")

    fields = require_mapping(issue.get("project_fields"), f"{key}.project_fields")
    status = fields.get("Status")
    if status != "Backlog":
        fail(f"{key} must initially have project field Status: Backlog")

    for field, value in fields.items():
        if field not in project_fields:
            fail(f"{key}.project_fields contains unsupported field: {field}")
        clean_value = require_clean_string(value, f"{key}.project_fields.{field}")
        if clean_value not in project_fields[field]:
            fail(
                f"{key}.project_fields.{field} contains unsupported option: "
                f"{clean_value}"
            )

    if "ItemType" in fields and fields["ItemType"] != issue_type:
        fail(f"{key}.project_fields.ItemType must match {key}.type")

    parent = issue.get("parent")
    if parent is not None:
        parent = require_clean_string(parent, f"{key}.parent")

    dependencies_value = issue.get("depends_on", [])
    if not isinstance(dependencies_value, list):
        fail(f"{key}.depends_on must be a list")
    dependencies: list[str] = []
    for dependency_index, dependency in enumerate(dependencies_value):
        dependencies.append(
            require_clean_string(
                dependency, f"{key}.depends_on[{dependency_index}]"
            )
        )
    if len(dependencies) != len(set(dependencies)):
        fail(f"{key}.depends_on must not contain duplicate keys")

    return key, issue_type, parent, dependencies


def visit(
    node: str,
    graph: dict[str, list[str]],
    visiting: set[str],
    visited: set[str],
) -> None:
    if node in visiting:
        fail(f"Dependency cycle detected at: {node}")
    if node in visited:
        return

    visiting.add(node)
    for dependency in sorted(graph[node]):
        visit(dependency, graph, visiting, visited)
    visiting.remove(node)
    visited.add(node)


def validate_issues(
    plan: dict[Any, Any], project_fields: dict[str, list[str]]
) -> tuple[set[str], dict[str, list[str]]]:
    issues_value = plan.get("issues")
    if not isinstance(issues_value, list) or not issues_value:
        fail("issues must be a non-empty list")

    keys: set[str] = set()
    relationships: dict[str, tuple[str | None, list[str]]] = {}
    for index, issue_value in enumerate(issues_value):
        issue = require_mapping(issue_value, f"issues[{index}]")
        key, _issue_type, parent, dependencies = validate_issue_fields(
            issue, index, project_fields
        )
        if key in keys:
            fail(f"Duplicate issue key: {key}")
        keys.add(key)
        relationships[key] = (parent, dependencies)

    graph = {
        key: dependencies
        for key, (_parent, dependencies) in relationships.items()
    }
    for key, (parent, dependencies) in relationships.items():
        if parent is not None:
            if parent == key:
                fail(f"{key} cannot parent itself")
            if parent not in keys:
                fail(f"{key} references missing parent: {parent}")
        for dependency in dependencies:
            if dependency not in keys:
                fail(f"{key} depends on missing key: {dependency}")
            if dependency == key:
                fail(f"{key} cannot depend on itself")

    visited: set[str] = set()
    for key in sorted(keys):
        visit(key, graph, set(), visited)
    return keys, graph


def validate_plan(plan: Any, require_approval: bool) -> int:
    if not isinstance(plan, dict):
        fail("Top-level YAML value must be a mapping")
    for key in plan:
        if not isinstance(key, str):
            fail("Top-level mapping keys must be strings")

    schema_version = plan.get("schema_version")
    if schema_version != SCHEMA_VERSION:
        fail(f"schema_version must be {SCHEMA_VERSION!r}")

    if plan.get("item_kind") != "repository_issue":
        fail("item_kind must be 'repository_issue'; draft Project items are unsupported")

    project = validate_project(plan)
    validate_approval(plan, require_approval)
    declared_project_fields = validate_required_project_fields(plan)
    if "target_version" in project:
        if project["target_version"] not in declared_project_fields["Target version"]:
            fail(
                "project.target_version has unsupported value: "
                f"{project['target_version']}"
            )
    validate_defaults(plan, declared_project_fields)
    keys, _graph = validate_issues(plan, declared_project_fields)

    return len(keys)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", required=True)
    parser.add_argument(
        "--require-approval",
        action="store_true",
        help="require explicit human approval metadata before GitHub writes",
    )
    args = parser.parse_args()

    path = Path(args.plan)
    if not path.is_file():
        fail(f"Plan file not found: {path}")

    plan = load_plan(path)
    try:
        issue_count = validate_plan(plan, args.require_approval)
    except SystemExit:
        raise
    except Exception as exc:  # pragma: no cover - last-resort no-traceback guard
        fail(f"Invalid work plan: {exc}")

    print(f"VALID: {path}")
    print(f"Issues: {issue_count}")
    print("Dependency cycles: none")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
