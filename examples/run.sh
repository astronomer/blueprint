#!/usr/bin/env bash
#
# Run any example against a local Airflow.
#
#   ./run.sh <example>
#
# Example:
#   ./run.sh runtime-params
#
# Airflow UI: http://localhost:8080 (no login required)
#
set -euo pipefail

EXAMPLES_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
    echo "usage: ./run.sh <example>" >&2
    echo >&2
    echo "Available examples:" >&2
    for dir in "${EXAMPLES_DIR}"/*/; do
        name="$(basename "${dir}")"
        [[ "${name}" == _* ]] && continue
        [[ -d "${dir}dags" ]] || continue
        echo "  ${name}" >&2
    done
    exit 1
}

[[ $# -ge 1 ]] || usage

EXAMPLE="$1"

if [[ ! -d "${EXAMPLES_DIR}/${EXAMPLE}/dags" ]]; then
    echo "error: no such example '${EXAMPLE}' (expected ${EXAMPLES_DIR}/${EXAMPLE}/dags)" >&2
    echo >&2
    usage
fi

WHEELS_DIR="${EXAMPLES_DIR}/${EXAMPLE}/.wheels"
REPO_ROOT="$(cd "${EXAMPLES_DIR}/.." && pwd)"

# Each example installs the released airflow-blueprint through its
# requirements.txt, exactly as your own project would. For local development we
# additionally build a wheel from this working tree; the example's Dockerfile
# installs it over the released version so you are running your changes.
# Delete .wheels/ (or use a clone without it) to test against the real release.
if command -v uv >/dev/null 2>&1; then
    echo "Building airflow-blueprint from the working tree..."
    rm -rf "${WHEELS_DIR}"
    uv build --wheel --quiet -o "${WHEELS_DIR}" "${REPO_ROOT}"
else
    echo "uv not found; the example will use the released airflow-blueprint." >&2
fi

export EXAMPLE
exec docker compose \
    -f "${EXAMPLES_DIR}/_runtime/docker-compose.yaml" \
    -p "blueprint-${EXAMPLE}" \
    up --build
