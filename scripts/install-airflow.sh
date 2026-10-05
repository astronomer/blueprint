#!/usr/bin/env bash
# Usage: scripts/install-airflow.sh <airflow-version> <python-version>
set -euo pipefail

airflow_version="$1"
python_version="$2"

airflow_constraints="$(mktemp)"
constraints="$(mktemp)"
curl -sSfL -o "$airflow_constraints" \
  "https://raw.githubusercontent.com/apache/airflow/constraints-${airflow_version}/constraints-${python_version}.txt"

uv run --no-project --python 3.12 --with packaging python - "$airflow_constraints" > "$constraints" <<'EOF'
import sys
import tomllib

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

with open("pyproject.toml", "rb") as f:
    project = tomllib.load(f)

dev_tools = {canonicalize_name(Requirement(d).name) for d in project["dependency-groups"]["dev"]}
our_specifiers = {
    canonicalize_name(req.name): req.specifier
    for req in map(Requirement, project["project"]["dependencies"])
}

with open(sys.argv[1]) as f:
    for line in f:
        name, pinned, version = line.strip().partition("==")
        key = canonicalize_name(name)
        ours = our_specifiers.get(key)
        if pinned and (key in dev_tools or (ours is not None and version not in ours)):
            continue
        sys.stdout.write(line)
EOF

export VIRTUAL_ENV="$PWD/.venv"
uv venv --clear --python "$python_version" "$VIRTUAL_ENV"
uv pip install \
  "apache-airflow==${airflow_version}" \
  -e . \
  -e tests/entry_point_package \
  --group dev \
  --constraints "$constraints"

installed="$(.venv/bin/python -c 'import airflow; print(airflow.__version__)')"
if [[ "$installed" != "$airflow_version" ]]; then
  echo "Expected Airflow ${airflow_version}, got ${installed}" >&2
  exit 1
fi
echo "Installed Airflow ${installed} on Python ${python_version}"
