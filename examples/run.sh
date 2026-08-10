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

# Each example installs the released airflow-blueprint through its own
# requirements.txt, exactly as your project would. The compose file then mounts
# this repository's blueprint/ over it, so you run your working tree rather
# than the release -- see the comments on the volumes in
# _runtime/docker-compose.yaml.

export EXAMPLE
exec docker compose \
    -f "${EXAMPLES_DIR}/_runtime/docker-compose.yaml" \
    -p "blueprint-${EXAMPLE}" \
    up --build
