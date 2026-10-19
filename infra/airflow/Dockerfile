# Airflow image with the providers this project needs baked in.
#
# Why not `_PIP_ADDITIONAL_REQUIREMENTS` in the compose file? That installs
# packages every time a container starts: slow, needs internet at runtime, and
# each restart could resolve a different version. Baking them into an image
# makes every Airflow component run identical, tested code. The official
# Airflow docs say the same: the env var is for quick experiments only.
#
# Kept in step with requirements-airflow.txt at the repo root.
FROM apache/airflow:3.0.2-python3.12

# The constraints file pins every transitive dependency to the set the Airflow
# team tested for this exact release. Never pip install into Airflow without it.
RUN pip install --no-cache-dir \
      "apache-airflow==3.0.2" \
      apache-airflow-providers-postgres \
      apache-airflow-providers-apache-kafka \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.2/constraints-3.12.txt"
