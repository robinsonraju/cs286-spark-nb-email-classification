#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
submit="${SPARK_HOME:+${SPARK_HOME}/bin/}spark-submit"
input="${1:-${script_dir}/src/main/resources/subjects-small.csv}"

exec "$submit" --master "${SPARK_MASTER:-local[*]}" \
  --class edu.sjsu.cs286.emailcf.spark.SparkEmailClassifier \
  "${script_dir}/target/nb-email-classifier-1.0.jar" "$input"
