#!/usr/bin/env bash

set -euo pipefail

version="${1:-0.1.0}"

declare -a jars=(
  "salesforce-pubsub-core/target/salesforce-pubsub-core-${version}.jar"
  "salesforce-pubsub-cli/target/salesforce-pubsub-cli-${version}.jar"
  "orbital-salesforce-connector/target/orbital-salesforce-pubsub-connector-${version}.jar"
  "examples/target/examples-${version}.jar"
)

for jar_file in "${jars[@]}"; do
  test -s "$jar_file"
  jar tf "$jar_file" | grep -qx 'META-INF/MANIFEST.MF'
done

# A Maven POM-packaged module has no JAR under target; its source POM is the
# published BOM descriptor and is the artifact checked by `mvn package`.
bom="bom/pom.xml"
test -s "$bom"
grep -Fq '<packaging>pom</packaging>' "$bom"
grep -Fq '<dependencyManagement>' "$bom"
for artifact_id in salesforce-pubsub-core salesforce-pubsub-cli orbital-salesforce-pubsub-connector; do
  grep -Fq "<artifactId>${artifact_id}</artifactId>" "$bom"
done

echo "Verified ${#jars[@]} Maven JAR artifacts and BOM ${bom}."
