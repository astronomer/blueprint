#!/usr/bin/env bash
#
# Regenerate the committed JSON schemas in schemas/.
#
#   ./regenerate-schemas.sh            rewrite the files
#   ./regenerate-schemas.sh --check    fail if they are out of date (for CI)
#
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

# One schema for the DAG file itself, plus one per blueprint.
BLUEPRINTS=(extract load)

generate_into() {
    local dir="$1"
    mkdir -p "${dir}"
    blueprint schema --dag-args -o "${dir}/dag.schema.json" >/dev/null
    for name in "${BLUEPRINTS[@]}"; do
        blueprint schema "${name}" -o "${dir}/${name}.schema.json" >/dev/null
    done
}

if [[ "${1:-}" == "--check" ]]; then
    tmp="$(mktemp -d)"
    trap 'rm -rf "${tmp}"' EXIT
    generate_into "${tmp}"
    if ! diff -rq "${tmp}" schemas >/dev/null 2>&1; then
        echo "Committed schemas are out of date. Run ./regenerate-schemas.sh" >&2
        diff -ru schemas "${tmp}" >&2 || true
        exit 1
    fi
    echo "Schemas are up to date."
else
    generate_into schemas
    echo "Wrote schemas to schemas/"
fi
