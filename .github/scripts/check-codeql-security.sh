#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 || -z "$1" ]]; then
  echo "Usage: $0 <CodeQL SARIF output directory>" >&2
  exit 2
fi

output_dir=$1
if [[ ! -d "$output_dir" ]]; then
  echo "CodeQL SARIF output directory is missing: $output_dir" >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required to inspect CodeQL SARIF output" >&2
  exit 1
fi

sarif_files=()
while IFS= read -r -d '' sarif_file; do
  sarif_files+=("$sarif_file")
done < <(find "$output_dir" -type f -name '*.sarif' -print0)
if [[ ${#sarif_files[@]} -eq 0 ]]; then
  echo "No CodeQL SARIF files found under: $output_dir" >&2
  exit 1
fi

status=0
for sarif_file in "${sarif_files[@]}"; do
  if ! jq -e '
    type == "object" and
    (.runs | type) == "array" and
    (.runs | length) > 0 and
    all(.runs[];
      (.tool | type) == "object" and
      (.tool.driver | type) == "object" and
      ((.results // []) | type) == "array" and
      ((.tool.driver.rules // []) | type) == "array"
    )
  ' "$sarif_file" >/dev/null 2>&1; then
    echo "Malformed or unsupported CodeQL SARIF: $sarif_file" >&2
    status=1
    continue
  fi

  findings=$(jq -r --arg file "$sarif_file" '
    def security_severity:
      ((.properties? // {})["security-severity"]?)
      | if . == null then null
        elif type == "number" then .
        elif type == "string" then (tonumber? // null)
        else null
        end;

    range(0; (.runs | length)) as $run_index
    | .runs[$run_index] as $run
    | ($run.tool.driver.rules // []) as $driver_rules
    | (($run.tool.extensions // []) | map(.rules // []) | add // []) as $extension_rules
    | ($driver_rules + $extension_rules) as $all_rules
    | $run.results[]? as $result
    | ($driver_rules[$result.ruleIndex]? // {}) as $indexed_rule
    | (([$all_rules[] | select(.id? == $result.ruleId)] | first) // {}) as $named_rule
    | (($result | security_severity)
       // ($indexed_rule | security_severity)
       // ($named_rule | security_severity)) as $severity
    | select($severity != null and $severity >= 7.0)
    | "\($file): run \($run_index), rule \($result.ruleId // "<unknown>"), security-severity \($severity), \($result.message.text // $result.message.markdown // "<no message>")"
  ' "$sarif_file")

  if [[ -n "$findings" ]]; then
    printf '%s\n' "$findings" >&2
    status=1
  fi
done

if [[ $status -ne 0 ]]; then
  echo "High/critical CodeQL security findings or invalid SARIF output detected." >&2
  exit "$status"
fi

echo "No high/critical CodeQL security findings detected across ${#sarif_files[@]} SARIF file(s)."
