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
config=$(terraform -chdir="$terraform_dir" output -json ledger_migration_config)

config_value() {
  jq -er --arg key "$1" '.[$key] | select(type == "string" and length > 0)' <<< "$config"
}

LEDGER_MIGRATION_IMAGE=$(config_value image)
LEDGER_DB_HOST=$(config_value db_host)
LEDGER_DB_USER=$(config_value db_user)
LEDGER_DB_NAME=$(config_value db_name)
AWS_REGION=$(config_value aws_region)
LEDGER_DB_SECRET_ARN=$(config_value secret_arn)
cluster_name=$(terraform -chdir="$terraform_dir" output -raw eks_cluster_name)
export LEDGER_MIGRATION_IMAGE LEDGER_DB_HOST LEDGER_DB_USER LEDGER_DB_NAME LEDGER_DB_SECRET_ARN AWS_REGION

if [[ ! "$LEDGER_MIGRATION_IMAGE" =~ @sha256:[0-9a-f]{64}$ || -z "$cluster_name" || "$cluster_name" == "null" ]]; then
  echo "Apply Terraform with EKS and a digest-pinned Ledger migration image first." >&2
  exit 1
fi

LEDGER_MIGRATION_NAME="ledger-migration-$(date +%s)-$RANDOM"
export LEDGER_MIGRATION_NAME
aws eks update-kubeconfig --region "$AWS_REGION" --name "$cluster_name"
kubectl create namespace praxis --dry-run=client -o yaml | kubectl apply -f -
kubectl label namespace praxis pod-security.kubernetes.io/enforce=restricted \
  pod-security.kubernetes.io/audit=restricted pod-security.kubernetes.io/warn=restricted --overwrite
kubectl -n praxis create serviceaccount ledger-migration --dry-run=client -o yaml | kubectl apply -f -

cleanup() {
  kubectl -n praxis delete job "$LEDGER_MIGRATION_NAME" --ignore-not-found --wait=false >/dev/null || true
}
trap cleanup EXIT

envsubst '$LEDGER_MIGRATION_NAME $LEDGER_MIGRATION_IMAGE $LEDGER_DB_HOST $LEDGER_DB_USER $LEDGER_DB_NAME $LEDGER_DB_SECRET_ARN $AWS_REGION' \
  < "$script_dir/ledger-migration-job.yaml" | kubectl apply -f - >/dev/null

echo "Waiting for Kubernetes Job $LEDGER_MIGRATION_NAME"
if ! kubectl -n praxis wait --for=condition=complete "job/$LEDGER_MIGRATION_NAME" --timeout=16m; then
  kubectl -n praxis logs "job/$LEDGER_MIGRATION_NAME" --all-containers=true || true
  echo "Ledger migration failed; check the Job and database before retrying." >&2
  exit 1
fi
kubectl -n praxis logs "job/$LEDGER_MIGRATION_NAME" --all-containers=true
echo "Ledger migration succeeded."
