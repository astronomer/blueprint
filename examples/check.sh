#!/usr/bin/env bash
#
# Validate every example. Run by CI, and useful locally after changing one.
#
#   ./check.sh
#
# For each example directory this runs `blueprint list` and `blueprint lint`,
# installing an example's package/ first if it ships one. Examples that
# deliberately contain invalid YAML are asserted to *fail* lint instead.
#
set -uo pipefail

EXAMPLES_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Examples whose `blueprint lint` is expected to exit non-zero, because the
# broken file is the thing being demonstrated.
EXPECT_LINT_FAILURE=(resilient-loading)

# `blueprint` and `python` are taken from PATH, so run this inside whatever
# environment has airflow-blueprint installed (`uv run examples/check.sh`).
if command -v uv >/dev/null 2>&1; then
    install_editable() { uv pip install -q -e "$1"; }
else
    install_editable() { pip install -q -e "$1"; }
fi

failures=0

fail() {
    echo "  FAIL: $1" >&2
    failures=$((failures + 1))
}

expects_lint_failure() {
    local name="$1"
    for expected in "${EXPECT_LINT_FAILURE[@]}"; do
        [[ "${expected}" == "${name}" ]] && return 0
    done
    return 1
}

for dir in "${EXAMPLES_DIR}"/*/; do
    name="$(basename "${dir}")"
    [[ "${name}" == _* ]] && continue
    [[ -d "${dir}dags" ]] || continue

    echo "== ${name}"

    if [[ -f "${dir}package/pyproject.toml" ]]; then
        echo "  installing package/"
        install_editable "${dir}package" || fail "${name}: package install"
    fi

    pushd "${dir}" >/dev/null || continue

    # Each Docker runtime installs only its own example's package, but this
    # script shares one environment across all of them. Disable entry-point
    # discovery unless the example is the one demonstrating it, so a package
    # installed above does not collide with another example's local blueprints.
    if [[ -f package/pyproject.toml ]]; then
        entry_points=(--entry-points)
    else
        entry_points=(--no-entry-points)
    fi

    blueprint list "${entry_points[@]}" >/dev/null || fail "${name}: blueprint list"

    if expects_lint_failure "${name}"; then
        if blueprint lint "${entry_points[@]}" >/dev/null 2>&1; then
            fail "${name}: lint was expected to fail but passed"
        else
            echo "  lint failed as expected"
        fi
    else
        blueprint lint "${entry_points[@]}" >/dev/null || fail "${name}: blueprint lint"
    fi

    # Optional per-example checks, picked up by convention.
    if [[ -f pytest.ini ]]; then
        python -m pytest -q >/dev/null || fail "${name}: pytest"
    fi
    if [[ -x regenerate-schemas.sh ]]; then
        ./regenerate-schemas.sh --check >/dev/null || fail "${name}: schemas out of date"
    fi

    popd >/dev/null || true
done

if [[ ${failures} -gt 0 ]]; then
    echo
    echo "${failures} example check(s) failed." >&2
    exit 1
fi

echo
echo "All example checks passed."
