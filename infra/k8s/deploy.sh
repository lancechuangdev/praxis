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
  echo "Apply infra/aws Terraform to provision EKS before deploying workloads." >&2
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

aws eks update-kubeconfig --region "$AWS_REGION" --name "$cluster_name"
kubectl create namespace praxis --dry-run=client -o yaml | kubectl apply -f -

kubectl -n praxis create configmap praxis-runtime \
  --from-literal="AWS_REGION=$AWS_REGION" \
  --from-literal="LEDGER_DB_HOST=$(runtime_value ledger_db_host)" \
  --from-literal="LEDGER_DB_NAME=$(runtime_value ledger_db_name)" \
  --from-literal="LEDGER_DB_SECRET_ARN=$(runtime_value ledger_runtime_secret_arn)" \
  --from-literal="MSK_BROKERS=$(runtime_value bootstrap_brokers_iam)" \
  --from-literal="LEDGER_COMMANDS_TOPIC=$(runtime_value ledger_commands_topic)" \
  --from-literal="LEDGER_CONSUMER_GROUP=$(runtime_value ledger_consumer_group)" \
  --from-literal="MATCHING_EVENTS_TOPIC=$(runtime_value matching_events_topic)" \
  --dry-run=client -o yaml | kubectl apply -f -

envsubst '$ORDER_IMAGE $LEDGER_IMAGE $MATCHING_IMAGE' < "$script_dir/workloads.yaml" | kubectl apply -f -
kubectl -n praxis rollout status deployment/ledger --timeout=5m
kubectl -n praxis rollout status deployment/matching --timeout=5m
kubectl -n praxis rollout status deployment/order --timeout=5m

echo "Kubernetes workloads are ready. Order is private; no public traffic was cut over."
