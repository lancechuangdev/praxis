#!/usr/bin/env bash
set -euo pipefail
project_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
: "${ISTIO_ARTIFACT_DIR:?Set ISTIO_ARTIFACT_DIR to a new directory for review artifacts}"
for command in helm sha256sum; do
  command -v "${command}" >/dev/null || { printf 'Missing command: %s\n' "${command}" >&2; exit 1; }
done
version=$(cat "${project_dir}/infra/istio/version")
[[ "${version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { printf 'Invalid pinned Istio version\n' >&2; exit 1; }
grep -Fxq "  tag: ${version}" "${project_dir}/infra/istio/istiod-values.yaml"
grep -Fxq "tag: ${version}" "${project_dir}/infra/istio/cni-values.yaml"
# A fresh directory avoids mixing artifacts from different chart versions.
mkdir "${ISTIO_ARTIFACT_DIR}"
cp "${project_dir}/infra/istio/istiod-values.yaml" "${ISTIO_ARTIFACT_DIR}/istiod-values.yaml"
cp "${project_dir}/infra/istio/cni-values.yaml" "${ISTIO_ARTIFACT_DIR}/cni-values.yaml"
printf '%s\n' "${version}" > "${ISTIO_ARTIFACT_DIR}/version"
for chart in base cni istiod; do
  helm pull "${chart}" --repo https://blob.istio.io/istio-release/charts \
    --version "${version}" --destination "${ISTIO_ARTIFACT_DIR}"
done
helm template istio-base "${ISTIO_ARTIFACT_DIR}/base-${version}.tgz" \
  --namespace istio-system --set defaultRevision=default > "${ISTIO_ARTIFACT_DIR}/base.yaml"
helm template istio-cni "${ISTIO_ARTIFACT_DIR}/cni-${version}.tgz" \
  --namespace istio-system --values "${ISTIO_ARTIFACT_DIR}/cni-values.yaml" > "${ISTIO_ARTIFACT_DIR}/cni.yaml"
helm template istiod "${ISTIO_ARTIFACT_DIR}/istiod-${version}.tgz" \
  --namespace istio-system --values "${ISTIO_ARTIFACT_DIR}/istiod-values.yaml" > "${ISTIO_ARTIFACT_DIR}/istiod.yaml"
(cd "${ISTIO_ARTIFACT_DIR}" && sha256sum version *.tgz *.yaml > SHA256SUMS)
printf 'Prepared Istio %s for review in %s; no cluster resources changed.\n' "${version}" "${ISTIO_ARTIFACT_DIR}"
