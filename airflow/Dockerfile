FROM apache/airflow:3.0.6

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    "apache-airflow-providers-amazon==9.30.0" \
    "apache-airflow-providers-google @ git+https://github.com/apache/airflow.git@refs/pull/68479/head#subdirectory=providers/google" \
    "google-cloud-aiplatform>=1.155.0" \
    "google-genai>=1.0.0"
