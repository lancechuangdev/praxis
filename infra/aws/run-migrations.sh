#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 ledger|outbox|all" >&2
  exit 2
}

[[ $# -eq 1 ]] || usage
case "$1" in
  ledger) services=(ledger) ;;
  outbox) services=(outbox) ;;
  all) services=(ledger outbox) ;;
  *) usage ;;
esac

for command in aws terraform jq; do
  command -v "$command" >/dev/null || { echo "Missing required command: $command" >&2; exit 1; }
done

cd "$(dirname "${BASH_SOURCE[0]}")"

cluster=$(terraform output -raw ecs_cluster_arn)
subnets=$(terraform output -json private_subnet_ids)
security_group=$(terraform output -raw cex_client_security_group_id)
network=$(jq -cn --argjson subnets "$subnets" --arg sg "$security_group" \
  '{awsvpcConfiguration:{subnets:$subnets,securityGroups:[$sg],assignPublicIp:"DISABLED"}}')

for service in "${services[@]}"; do
  task_definition=$(terraform output -raw "${service}_migration_task_definition_arn")
  if [[ -z "$task_definition" || "$task_definition" == "null" ]]; then
    echo "No $service migration task definition. Set ${service}_migration_image_digest and apply Terraform first." >&2
    exit 1
  fi

  echo "Starting $service migration: $task_definition"
  result=$(aws ecs run-task --cluster "$cluster" --task-definition "$task_definition" \
    --launch-type FARGATE --count 1 --network-configuration "$network" --output json)
  if ! jq -e '(.failures | length) == 0 and (.tasks | length) == 1' <<<"$result" >/dev/null; then
    jq '{failures, tasks}' <<<"$result" >&2
    exit 1
  fi
  task_arn=$(jq -r '.tasks[0].taskArn' <<<"$result")
  echo "Waiting for $task_arn"
  aws ecs wait tasks-stopped --cluster "$cluster" --tasks "$task_arn"

  result=$(aws ecs describe-tasks --cluster "$cluster" --tasks "$task_arn" --output json)
  if ! jq -e '(.failures | length) == 0 and (.tasks | length) == 1 and
    (.tasks[0].containers | length) == 1 and .tasks[0].containers[0].exitCode == 0' \
    <<<"$result" >/dev/null; then
    jq '{failures, tasks: [.tasks[] | {taskArn, stoppedReason, containers: [.containers[] | {name, exitCode, reason}]}]}' \
      <<<"$result" >&2
    exit 1
  fi
  echo "$service migration succeeded."
done
