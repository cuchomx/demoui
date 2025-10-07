#!/usr/bin/env bash
set -euo pipefail

AWS_ENDPOINT="http://localhost:4566"

create_queue() {
  local queue_name="$1"
  echo "Creating SQS queue: ${queue_name}"
  aws --endpoint-url="${AWS_ENDPOINT}" sqs create-queue --queue-name "${queue_name}"
}

QUEUE_NAMES=(
  "product-create-web"
  "product-create-service"
  "product-find-web"
  "product-find-service"
)

for q in "${QUEUE_NAMES[@]}"; do
  create_queue "${q}"
done

echo "All queues created successfully."
