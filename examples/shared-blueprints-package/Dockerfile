# A standard Astro Runtime project image, as `astro dev init` generates.
FROM astrocrpublic.azurecr.io/runtime:3.3

# The base image installs packages.txt and requirements.txt and copies this
# project into /usr/local/airflow.
#
# A real consumer repo would install the platform team's package from its
# package index, by listing `acme-blueprints` in requirements.txt. Here the
# package is a sibling directory in this repository, so it is installed from
# source instead. Either way its blueprints are then discovered through the
# entry point it declares -- no Airflow-side configuration needed.
USER root
RUN pip install --no-deps -e ./package
USER astro
