FROM apache/airflow:2.8.4-python3.11

# Keep the runtime user non-root, matching the upstream Airflow image.
USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY --chown=airflow:root . /opt/airflow/project

ENV PYTHONPATH=/opt/airflow/project
WORKDIR /opt/airflow/project
