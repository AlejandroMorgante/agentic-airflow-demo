FROM apache/airflow:3.0.6

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
ARG AIRFLOW_PROVIDER_REF=2c7240709240b0052e016178e4eb37f4ca01ad82
RUN pip install --no-cache-dir \
    "apache-airflow-providers-amazon==9.30.0" \
    "apache-airflow-providers-google @ git+https://github.com/apache/airflow.git@${AIRFLOW_PROVIDER_REF}#subdirectory=providers/google" \
    "apache-airflow-providers-microsoft-azure @ git+https://github.com/apache/airflow.git@86fce9688156b242e7df9c31f01ad4dfa84663b6#subdirectory=providers/microsoft/azure" \
    "google-cloud-aiplatform>=1.155.0" \
    "google-genai>=1.0.0"
