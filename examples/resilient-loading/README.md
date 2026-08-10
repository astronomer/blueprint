# Resilient Loading

Keeping one bad YAML file from taking down every other DAG in the folder, and keeping
work-in-progress out of Airflow entirely.

## Why you'd do this

All the YAML in a folder is built by one loader, so the effect of one bad file is not limited to
that file. By default a single unbuildable file aborts the load and every DAG beside it
disappears from the UI, which means a typo in a new pipeline can remove unrelated production
DAGs.

There are two separate problems here, with two separate answers. A file that is broken *by
accident* should be contained. A file that is not finished *on purpose* should never be loaded
at all.

## Files

| File | What it does |
|---|---|
| `dags/loader.py` | `build_all_airflow_dags(skip_invalid_dags=True)` |
| `dags/healthy.dag.yaml` | Builds normally |
| `dags/broken.dag.yaml` | Missing a required field; logged and skipped |
| `dags/.airflowignore` | Excludes `drafts/` |
| `dags/drafts/wip.dag.yaml` | Never built, and invalid — safely |

## Run it

```bash
../run.sh resilient-loading
```

`blueprint lint` **fails in this directory on purpose** — `broken.dag.yaml` really is broken.
That is the difference between the two tools, and the point of the last section below.

## Walk-through

### Containing accidental breakage

```python
build_all_airflow_dags(skip_invalid_dags=True)
```

Files that fail to parse or build are logged and skipped; the rest load as usual:

```
INFO  - Built DAG 'healthy_pipeline' from healthy.dag.yaml
INFO  - Successfully built 1 DAG(s): healthy_pipeline
WARNING - Skipped 1 invalid DAG file(s); built 1 DAG(s) successfully
```

Without the flag, the `LoadConfig` validation error propagates out of the loader, Airflow marks
the whole file as failed to import, and `healthy_pipeline` vanishes too.

This is a containment measure, not a fix. The failure is reported only as a warning in the
scheduler log, so a skipped DAG is one that has stopped running without any visible signal. Two
things make that acceptable:

- **Lint in CI**, so a broken file cannot merge in the first place. The flag is for what slips
  through, not a substitute. See [editor-and-ci](../editor-and-ci/).
- **Alert on the warning**, or on DAGs that disappear from the list.

Whether to enable it at all is a judgement call. A repo where many teams commit to one dags
folder wants it. A repo where one team owns everything might prefer to fail loudly.

### Excluding work in progress

```
# dags/.airflowignore
drafts
```

`.airflowignore` uses exactly Airflow's own syntax and semantics — the
`core.dag_ignore_file_syntax` setting for glob vs regexp, and nested ignore files in
subdirectories. Anything matched is not discovered, so `drafts/wip.dag.yaml` is never built.

Notice it is also invalid, and no warning is logged for it. That is the distinction:
`skip_invalid_dags` handles files that *should* have worked, `.airflowignore` handles files
nobody is claiming yet. A half-finished DAG belongs in `drafts/`, not in the folder above
relying on the skip flag.

### Lint follows the same rules — with an escape hatch

Directory-wide `blueprint lint` honours `.airflowignore` the way the loader does:

```
$ blueprint lint
FAIL dags/broken.dag.yaml
  Error: 1 validation error for LoadConfig
target
  Field required [type=missing, input_value={}, input_type=dict]
PASS dags/healthy.dag.yaml (dag_id=healthy_pipeline)
```

`drafts/wip.dag.yaml` is absent — ignored, not passed. To check an ignored file anyway, name it
explicitly:

```bash
blueprint lint dags/drafts/wip.dag.yaml
```

An explicit path always wins over an ignore entry, which is what you want while working on a
draft: full validation on demand, no risk of it loading.

Note that lint does **not** honour `skip_invalid_dags` — it exits non-zero if anything fails,
which is precisely what makes it useful in CI.

## What to look at in the UI

Only `healthy_pipeline` appears. Neither `broken_pipeline` nor `wip_pipeline` is there, and
they are absent for different reasons — check the scheduler or DAG-processor logs for the
`Skipped 1 invalid DAG file(s)` warning, which mentions `broken.dag.yaml` and says nothing at
all about `drafts/`.

Try deleting `skip_invalid_dags=True` from the loader and reloading: `healthy_pipeline`
disappears too.

## Related

- [editor-and-ci](../editor-and-ci/) — stopping broken YAML from merging
- [config-validation](../config-validation/) — why `broken.dag.yaml` fails
- [testing-blueprints](../testing-blueprints/) — asserting every DAG in a repo still builds
