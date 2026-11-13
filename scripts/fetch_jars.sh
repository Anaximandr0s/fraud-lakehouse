#!/usr/bin/env bash
# Downloads to a .part file first, so an interrupted run never leaves a truncated jar behind.
set -euo pipefail

MAVEN=https://repo1.maven.org/maven2

fetch() {
  local dir=$1 group=$2 artifact=$3 version=$4
  local jar="$dir/$artifact-$version.jar"
  local url="$MAVEN/$group/$artifact/$version/$artifact-$version.jar"
  if [[ ! -f $jar ]]; then
    mkdir -p "$dir"
    echo "downloading $artifact-$version.jar"
    curl -fsSL --retry 5 --retry-all-errors -C - -o "$jar.part" "$url"
    mv "$jar.part" "$jar"
  fi
  # Checked on every run, so a jar corrupted after download is caught too.
  local expected actual
  expected=$(curl -fsSL --retry 5 "$url.sha1" | cut -d' ' -f1)
  actual=$(sha1sum "$jar" | cut -d' ' -f1)
  if [[ $actual != "$expected" ]]; then
    rm -f "$jar"
    echo "sha1 mismatch for $jar (expected $expected, got $actual): deleted" >&2
    exit 1
  fi
}

fetch jars/hive org/postgresql postgresql "$POSTGRES_JDBC_VERSION"

fetch jars/spark org/apache/hadoop hadoop-aws "$SPARK_HADOOP_AWS_VERSION"
fetch jars/spark com/amazonaws aws-java-sdk-bundle "$SPARK_AWS_SDK_V1_VERSION"
fetch jars/spark org/apache/iceberg iceberg-spark-runtime-3.5_2.12 "$ICEBERG_VERSION"
fetch jars/spark org/apache/spark spark-sql-kafka-0-10_2.12 "$SPARK_KAFKA_VERSION"
fetch jars/spark org/apache/spark spark-token-provider-kafka-0-10_2.12 "$SPARK_KAFKA_VERSION"
fetch jars/spark org/apache/kafka kafka-clients "$KAFKA_CLIENTS_VERSION"
fetch jars/spark org/apache/commons commons-pool2 "$COMMONS_POOL2_VERSION"
