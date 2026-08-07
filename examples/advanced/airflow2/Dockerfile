FROM quay.io/astronomer/astro-runtime:13.4.0-base

USER root

RUN pip install -U uv

COPY ./examples/advanced/requirements.txt ${AIRFLOW_HOME}/requirements.txt
COPY ./pyproject.toml ${AIRFLOW_HOME}/blueprint/pyproject.toml
COPY ./README.md ${AIRFLOW_HOME}/blueprint/README.md
COPY ./blueprint ${AIRFLOW_HOME}/blueprint/blueprint
COPY ./examples/shared-blueprints ${AIRFLOW_HOME}/shared-blueprints

RUN uv pip install --system -e "${AIRFLOW_HOME}/blueprint"

# A central team's blueprint package, installed like any other dependency.
# Its blueprints are discovered automatically via the entry point it
# declares in shared-blueprints/pyproject.toml -- no config needed here.
RUN uv pip install --system -e "${AIRFLOW_HOME}/shared-blueprints"

RUN uv pip install --system -r "${AIRFLOW_HOME}/requirements.txt"

RUN chown -R astro:astro ${AIRFLOW_HOME}/blueprint ${AIRFLOW_HOME}/shared-blueprints

USER astro
