FROM astrocrpublic.azurecr.io/runtime:3.1-12-base

USER root

RUN pip install -U uv

COPY ./examples/requirements.txt ${AIRFLOW_HOME}/requirements.txt
COPY ./pyproject.toml ${AIRFLOW_HOME}/blueprint/pyproject.toml
COPY ./README.md ${AIRFLOW_HOME}/blueprint/README.md
COPY ./blueprint ${AIRFLOW_HOME}/blueprint/blueprint

RUN uv pip install --system -e "${AIRFLOW_HOME}/blueprint"

RUN uv pip install --system -r "${AIRFLOW_HOME}/requirements.txt"

RUN chown -R astro:astro ${AIRFLOW_HOME}/blueprint

USER astro
