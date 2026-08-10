# A standard Astro Runtime project image.
#
# BASE_IMAGE is an argument only so this one file can run the example on both
# Airflow 2 and Airflow 3. In your own project you would write the FROM line
# directly:
#
#     FROM astrocrpublic.azurecr.io/runtime:3.3
#
ARG BASE_IMAGE=astrocrpublic.azurecr.io/runtime:3.3
FROM ${BASE_IMAGE}

# Nothing else is needed. The base image installs packages.txt and
# requirements.txt and copies this project into /usr/local/airflow for you --
# which is where airflow-blueprint comes from.

# ---------------------------------------------------------------------------
# Everything below is specific to the Blueprint repository and has no place in
# a real project. `examples/run.sh` builds a wheel from the working tree into
# .wheels/, and this installs it over the released version from
# requirements.txt so the example exercises local changes.
# ---------------------------------------------------------------------------
USER root
RUN if ls .wheels/airflow_blueprint-*.whl >/dev/null 2>&1; then \
        pip install --no-deps --force-reinstall .wheels/airflow_blueprint-*.whl; \
    fi
USER astro
