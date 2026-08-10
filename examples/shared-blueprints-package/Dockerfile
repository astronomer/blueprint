# A standard Astro Runtime project image, as `astro dev init` generates.
FROM astrocrpublic.azurecr.io/runtime:3.3

# The base image installs packages.txt and requirements.txt and copies this
# project into /usr/local/airflow for you.
#
# A real consumer repo would install the platform team's package from its
# package index, by listing `acme-blueprints` in requirements.txt above. Here
# the package is a sibling directory in this repository, so it is installed
# from source instead. Either way its blueprints are then discovered through
# the entry point it declares -- no Airflow-side configuration needed.
USER root
RUN pip install --no-deps -e ./package
USER astro

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
