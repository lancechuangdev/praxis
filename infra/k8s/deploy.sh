#!/usr/bin/env bash
set -euo pipefail

for command_name in aws envsubst jq kubectl terraform; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "Missing required command: $command_name" >&2
    exit 1
  fi
done

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
terraform_dir="${script_dir}/../aws"

cluster_name=$(terraform -chdir="$terraform_dir" output -raw eks_cluster_name)
if [[ -z "$cluster_name" || "$cluster_name" == "null" ]]; then
  echo "Enable EKS and apply infra/aws Terraform before deploying workloads." >&2
  exit 1
fi

runtime_json=$(terraform -chdir="$terraform_dir" output -json eks_runtime_config)
images_json=$(terraform -chdir="$terraform_dir" output -json eks_image_refs)

runtime_value() {
  jq -er --arg key "$1" '.[$key] | select(type == "string" and length > 0)' <<< "$runtime_json"
}

AWS_REGION=$(runtime_value aws_region)
export AWS_REGION
ORDER_IMAGE=$(jq -er '.order | select(type == "string" and length > 0)' <<< "$images_json")
LEDGER_IMAGE=$(jq -er '.ledger | select(type == "string" and length > 0)' <<< "$images_json")
MATCHING_IMAGE=$(jq -er '.matching | select(type == "string" and length > 0)' <<< "$images_json")
export ORDER_IMAGE LEDGER_IMAGE MATCHING_IMAGE

if [[ ! "$ORDER_IMAGE" =~ @sha256:[0-9a-f]{64}$ || ! "$LEDGER_IMAGE" =~ @sha256:[0-9a-f]{64}$ || ! "$MATCHING_IMAGE" =~ @sha256:[0-9a-f]{64}$ ]]; then
  echo "All three EKS images must be pinned to ECR sha256 digests." >&2
  exit 1
fi

ecs_cluster=$(terraform -chdir="$terraform_dir" output -raw ecs_cluster_name)
matching_services=()
ledger_services=()
for output_name in matching_ecs_service_arn matching_ec2_service_arn; do
  service_arn=$(terraform -chdir="$terraform_dir" output -json "$output_name" | jq -r '. // empty')
  if [[ -n "$service_arn" ]]; then
    matching_services+=("$service_arn")
  fi
done
for output_name in ledger_ecs_service_arn ledger_ec2_service_arn; do
  service_arn=$(terraform -chdir="$terraform_dir" output -json "$output_name" | jq -r '. // empty')
  if [[ -n "$service_arn" ]]; then
    ledger_services+=("$service_arn")
  fi
done

if ((${#matching_services[@]} > 0)); then
  if ! aws ecs describe-services --region "$AWS_REGION" --cluster "$ecs_cluster" --services "${matching_services[@]}" \
    | jq -e '(.failures | length) == 0 and all(.services[]; .desiredCount == 0 and .runningCount == 0 and .pendingCount == 0)' >/dev/null; then
    echo "Stop all ECS Matching tasks before deploying the Kubernetes mock; it has no partition fencing." >&2
    exit 1
  fi
fi

if ((${#ledger_services[@]} > 0)) && [[ "${ALLOW_LEDGER_PARALLEL_CONSUMER:-false}" != "true" ]]; then
  if ! aws ecs describe-services --region "$AWS_REGION" --cluster "$ecs_cluster" --services "${ledger_services[@]}" \
    | jq -e '(.failures | length) == 0 and all(.services[]; .desiredCount == 0 and .runningCount == 0 and .pendingCount == 0)' >/dev/null; then
    echo "ECS Ledger is active. Its Kubernetes peer would join the live Kafka group." >&2
    echo "Stop ECS Ledger first, or explicitly set ALLOW_LEDGER_PARALLEL_CONSUMER=true." >&2
    exit 1
  fi
fi

aws eks update-kubeconfig --region "$AWS_REGION" --name "$cluster_name"
kubectl create namespace praxis --dry-run=client -o yaml | kubectl apply -f -

kubectl -n praxis create configmap praxis-runtime \
  --from-literal="AWS_REGION=$AWS_REGION" \
  --from-literal="LEDGER_DB_HOST=$(runtime_value ledger_db_host)" \
  --from-literal="LEDGER_DB_NAME=$(runtime_value ledger_db_name)" \
  --from-literal="MSK_BROKERS=$(runtime_value bootstrap_brokers_iam)" \
  --from-literal="LEDGER_COMMANDS_TOPIC=$(runtime_value ledger_commands_topic)" \
  --from-literal="LEDGER_CONSUMER_GROUP=$(runtime_value ledger_consumer_group)" \
  --from-literal="MATCHING_EVENTS_TOPIC=$(runtime_value matching_events_topic)" \
  --dry-run=client -o yaml | kubectl apply -f -

# Stream the password into kubectl; never put it in argv, a manifest, or Terraform state.
secret_arn=$(runtime_value ledger_runtime_secret_arn)
aws secretsmanager get-secret-value --region "$AWS_REGION" --secret-id "$secret_arn" \
  --query SecretString --output text \
  | jq -erj '.password | select(type == "string" and length > 0)' \
  | kubectl -n praxis create secret generic ledger-runtime \
      --from-file=LEDGER_DB_PASSWORD=/dev/stdin --dry-run=client -o yaml \
  | kubectl apply -f -

envsubst '$ORDER_IMAGE $LEDGER_IMAGE $MATCHING_IMAGE' < "$script_dir/workloads.yaml" | kubectl apply -f -
kubectl -n praxis rollout status deployment/ledger --timeout=5m
kubectl -n praxis rollout status deployment/matching --timeout=5m
kubectl -n praxis rollout status deployment/order --timeout=5m

echo "Kubernetes workloads are ready. Order is private; no public traffic was cut over."
