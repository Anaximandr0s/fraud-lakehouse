#!/usr/bin/env bash
# Downloads to a .part file first, so an interrupted run never leaves a truncated jar behind.
set -euo pipefail

MAVEN=https://repo1.maven.org/maven2

fetch() {
  local dir=$1 group=$2 artifact=$3 version=$4
  local jar="$dir/$artifact-$version.jar"
  [[ -f $jar ]] && return
  mkdir -p "$dir"
  echo "downloading $artifact-$version.jar"
  curl -fsSL --retry 5 --retry-all-errors -C - -o "$jar.part" "$MAVEN/$group/$artifact/$version/$artifact-$version.jar"
  mv "$jar.part" "$jar"
}

fetch jars/hive org/postgresql postgresql "$POSTGRES_JDBC_VERSION"

fetch jars/spark org/apache/hadoop hadoop-aws "$SPARK_HADOOP_AWS_VERSION"
fetch jars/spark com/amazonaws aws-java-sdk-bundle "$SPARK_AWS_SDK_V1_VERSION"
fetch jars/spark org/apache/iceberg iceberg-spark-runtime-3.5_2.12 "$ICEBERG_VERSION"
