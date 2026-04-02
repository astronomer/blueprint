---
name: release
description: Create and publish a new release of airflow-blueprint. Bumps version, runs checks, opens a PR, merges, tags, publishes to PyPI, and creates GitHub release notes.
disable-model-invocation: true
user-invocable: true
argument-hint: [version]
---

# Release v$ARGUMENTS

You are releasing version $ARGUMENTS of the `airflow-blueprint` package. Follow every step in order. Do not skip steps. Stop and ask the user if anything fails.

## 1. Validate the version argument

- $ARGUMENTS must be a valid semver string (e.g. `0.3.0`, `1.0.0`). Reject pre-release suffixes unless the user explicitly asked for one.
- Read `blueprint/__init__.py` and confirm the current `__version__` is older than $ARGUMENTS.

## 2. Run local quality checks

Run all three in parallel:

- `uv run ruff check blueprint/ tests/`
- `uv run ruff format --check blueprint/ tests/`
- `uv run pytest tests/ -v` (unit **and** integration tests)

All must pass before continuing.

## 3. Create a release branch and bump the version

- `git checkout -b release/v$ARGUMENTS`
- Edit `blueprint/__init__.py`: set `__version__ = "$ARGUMENTS"`
- `git add blueprint/__init__.py`
- `git commit -m "Release v$ARGUMENTS"`
- `git push -u origin release/v$ARGUMENTS`

## 4. Open a PR

```
gh pr create --title "Release v$ARGUMENTS" --body "Bump version to $ARGUMENTS."
```

## 5. Wait for CI to pass

- `gh pr checks <PR_NUMBER> --watch`
- All checks must pass. If any fail, investigate and fix before continuing.

## 6. Merge the PR

```
gh pr merge <PR_NUMBER> --squash
```

## 7. Tag the merge commit on main

- `git checkout main && git pull`
- `git tag v$ARGUMENTS`
- `git push origin v$ARGUMENTS`

This triggers the release workflow which builds and publishes to PyPI and TestPyPI.

## 8. Wait for the release workflow to pass

- Find the workflow run: `gh run list --branch v$ARGUMENTS --limit 1`
- Watch it: `gh run watch <RUN_ID>`
- Confirm that the **Publish to PyPI** and **Publish to TestPyPI** jobs both succeed.

## 9. Draft release notes

Before creating the release, analyze all commits since the last tag to draft release notes. Use `git log <previous_tag>..v$ARGUMENTS --oneline` and read the relevant diffs to understand each change.

Structure the notes as:

```
## What's new
### Feature title (PR #)
Short description of the feature and how to use it.

(repeat for each feature)

---

## Breaking changes
### Change title
Description of what changed, what the old behavior was, and how to migrate.

(only include this section if there are breaking changes)

---

**Full Changelog**: https://github.com/astronomer/blueprint/compare/<previous_tag>...v$ARGUMENTS
```

Guidelines:
- Group related PRs together under a single heading when they are part of the same feature.
- Omit purely internal changes (CI, test infra, docs-only) from the top-level notes unless they are significant to users.
- For breaking changes, always include a before/after code example showing the migration path.

## 10. Create the GitHub release

```
gh release create v$ARGUMENTS --title "v$ARGUMENTS" --notes "<drafted notes>"
```

## 11. Clean up

- Delete the local release branch: `git branch -d release/v$ARGUMENTS`
- Report the release URL and a summary to the user.
